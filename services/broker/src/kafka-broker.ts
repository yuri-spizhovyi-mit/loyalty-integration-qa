import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  Consumer,
  type Message as KafkaMessage,
  ProduceAcks,
  Producer,
  stringDeserializers,
  stringSerializers,
} from '@platformatic/kafka';
import { dltTopic, newMessageId } from '../../shared/kafka.js';
import { customerIdOf, type DeadLetter, type Delivery, type Message, type MessageBroker } from './broker.js';

/**
 * The broker service when the system runs on Kafka. The services produce
 * to and consume from Kafka themselves; this service is only the test-facing
 * view of it, with the same HTTP API as the in-memory broker:
 *
 * - publish: produce a record (key = customerId) the way a service would.
 * - messages: an index of every record on the topics and dead-letter topics,
 *   built by a consumer group of its own that reads from the beginning.
 * - deliveries: asked from each consuming service, which records what it did
 *   with every record (attempts, outcome, dead-lettering).
 * - redrive: produce a dead-lettered record back to its source topic, with
 *   the same message-id.
 *
 * Reads are "read your writes": before answering, the index catches up to
 * the topic's end offsets as of the request, so a record produced by a
 * service just before the request is included.
 */

export interface KafkaBinding {
  /** Also the consumer group name. */
  queue: string;
  topic: string;
  /** Base URL of the consuming service, for /internal/deliveries and /internal/consumer. */
  consumerUrl: string;
}

interface IndexedRecord {
  message: Omit<Message, 'deliveries'>;
  headers: Map<string, string>;
  key: string | null;
  timestamp: number;
}

const CATCH_UP_TIMEOUT_MS = 5_000;

export class KafkaBroker implements MessageBroker {
  readonly backend = 'kafka' as const;
  private readonly producer: Producer<string, string, string, string>;
  private index: Consumer<string, string, string, string>;
  private readonly records = new Map<string, IndexedRecord>();
  /** Latest DLT record per queue and message-id. */
  private readonly deadLettered = new Map<string, Map<string, IndexedRecord>>();
  private readonly redriven = new Map<string, number>();
  /** Next offset to read, per "topic:partition". */
  private readonly position = new Map<string, bigint>();
  private joined = false;

  constructor(
    private readonly bindings: KafkaBinding[],
    private readonly brokers: string[],
  ) {
    this.producer = new Producer({
      clientId: 'broker-api',
      bootstrapBrokers: brokers,
      serializers: stringSerializers,
      idempotent: true,
      acks: ProduceAcks.ALL,
      autocreateTopics: false,
    });
    this.index = this.createIndex();
    for (const { queue } of bindings) this.deadLettered.set(queue, new Map());
  }

  /** A fresh group each time, reading from the beginning, (re)builds the index. */
  private createIndex(): Consumer<string, string, string, string> {
    const index = new Consumer<string, string, string, string>({
      clientId: 'broker-index',
      groupId: `broker-index-${randomUUID()}`,
      bootstrapBrokers: this.brokers,
      deserializers: stringDeserializers,
      autocommit: false,
      maxWaitTime: 100,
    });
    index.on('consumer:group:join', () => (this.joined = true));
    return index;
  }

  get ready(): boolean {
    return this.joined;
  }

  private get topics(): string[] {
    return [...new Set(this.bindings.map((b) => b.topic)), ...this.bindings.map((b) => dltTopic(b.queue))];
  }

  /** Keeps the index consumer running; restarts it if its stream fails (as the consumers do). */
  async start(): Promise<void> {
    for (;;) {
      const index = this.index;
      try {
        const stream = await index.consume({ topics: this.topics, mode: 'earliest' });
        await new Promise<void>((resolve) => {
          stream.on('data', (record) => this.indexRecord(record));
          stream.once('error', (error) => {
            console.error(`[broker] index stream failed, restarting: ${String(error)}`);
            resolve();
          });
          stream.once('close', () => resolve());
        });
      } catch (error) {
        console.error(`[broker] cannot start the index consumer yet: ${String(error)}`);
      }
      this.joined = false;
      await index.close(true).catch(() => undefined);
      await sleep(1000);
      this.index = this.createIndex();
    }
  }

  hasTopic(topic: string): boolean {
    return this.bindings.some((b) => b.topic === topic);
  }

  hasQueue(queue: string): boolean {
    return this.bindings.some((b) => b.queue === queue);
  }

  async publish(topic: string, payload: string, correlationId: string | null): Promise<Message> {
    const messageId = newMessageId();
    const customerId = customerIdOf(payload);
    const headers: Record<string, string> = { 'message-id': messageId };
    if (correlationId) headers['correlation-id'] = correlationId;
    const result = await this.producer.send({
      messages: [{ topic, key: customerId ?? undefined, value: payload, headers }],
    });
    const written = result.offsets?.[0];
    const message: Omit<Message, 'deliveries'> = {
      messageId,
      topic,
      payload,
      customerId,
      correlationId,
      publishedAt: new Date().toISOString(),
      partition: written?.partition ?? null,
      offset: written ? String(written.offset) : null,
    };
    if (!this.records.has(messageId)) {
      this.records.set(messageId, { message, headers: new Map(Object.entries(headers)), key: customerId, timestamp: Date.now() });
    }
    return { ...message, deliveries: this.queuesFor(topic).map((queue) => pendingDelivery(queue)) };
  }

  async getMessage(messageId: string): Promise<Message | undefined> {
    if (!this.records.has(messageId)) await this.catchUp(this.topics);
    const record = this.records.get(messageId);
    if (!record) return undefined;
    return { ...record.message, deliveries: await this.deliveriesOf(record.message) };
  }

  async messagesOn(topic: string, customerId?: string): Promise<Message[]> {
    await this.catchUp([topic]);
    const matching = [...this.records.values()]
      .filter((r) => r.message.topic === topic && (!customerId || r.message.customerId === customerId))
      .sort((a, b) => a.timestamp - b.timestamp || Number(a.message.offset) - Number(b.message.offset));
    return Promise.all(matching.map(async (r) => ({ ...r.message, deliveries: await this.deliveriesOf(r.message) })));
  }

  async deadLetters(queue: string, filter: { customerId?: string; messageId?: string }): Promise<DeadLetter[]> {
    await this.catchUp([dltTopic(queue)]);
    const result: DeadLetter[] = [];
    for (const [messageId, dlt] of this.deadLettered.get(queue) ?? []) {
      if (filter.messageId && messageId !== filter.messageId) continue;
      if ((this.redriven.get(`${queue}:${messageId}`) ?? 0) > dlt.timestamp) continue; // redriven since
      const original = this.records.get(messageId)?.message ?? this.messageFromDlt(messageId, dlt);
      if (filter.customerId && original.customerId !== filter.customerId) continue;
      const delivery = await this.deliveryOf(queue, original.topic, messageId);
      result.push({
        ...original,
        deliveries: [delivery],
        delivery,
        // Where the DLT record itself lives, and what it says about the failure.
        deadLetterRecord: {
          topic: dltTopic(queue),
          partition: dlt.message.partition,
          offset: dlt.message.offset,
          key: dlt.key,
          payload: dlt.message.payload,
          headers: Object.fromEntries(dlt.headers),
        },
      });
    }
    return result;
  }

  /**
   * Produces the dead-lettered record back to its source topic. Every
   * consumer group on that topic sees it again; here each topic has one
   * group. With several, redrive would go to a retry topic per group instead.
   */
  async redrive(queue: string, messageId: string): Promise<Message | undefined> {
    await this.catchUp([dltTopic(queue)]);
    const dlt = this.deadLettered.get(queue)?.get(messageId);
    if (!dlt || (this.redriven.get(`${queue}:${messageId}`) ?? 0) > dlt.timestamp) return undefined;
    const sourceTopic = dlt.headers.get('dlt-source-topic') ?? this.bindings.find((b) => b.queue === queue)!.topic;
    const headers: Record<string, string> = { 'message-id': messageId, redriven: 'true' };
    const correlationId = dlt.headers.get('correlation-id');
    if (correlationId) headers['correlation-id'] = correlationId;
    this.redriven.set(`${queue}:${messageId}`, Date.now());
    await this.producer.send({
      messages: [{ topic: sourceTopic, key: dlt.key ?? undefined, value: dlt.message.payload, headers }],
    });
    return this.records.get(messageId)
      ? { ...this.records.get(messageId)!.message, deliveries: [pendingDelivery(queue)] }
      : { ...this.messageFromDlt(messageId, dlt), deliveries: [pendingDelivery(queue)] };
  }

  async stats() {
    return Promise.all(
      this.bindings.map(async ({ queue, topic, consumerUrl }) => {
        const consumer = await getJson<{
          ready: boolean;
          retryMode: string;
          lag: number[];
          committedOffsets: Record<string, string>;
        }>(
          `${consumerUrl}/internal/consumer`,
        );
        return {
          queue,
          topic,
          consumerGroup: queue,
          deadLetterTopic: dltTopic(queue),
          retryMode: consumer?.retryMode ?? null,
          lagByPartition: consumer?.lag ?? null,
          lag: consumer ? consumer.lag.reduce((sum, value) => sum + value, 0) : null,
          committedOffsets: consumer?.committedOffsets ?? null,
          // Counted from the index alone: no per-record lookups, so this stays fast as the DLT grows.
          deadLetters: this.openDeadLetterCount(queue),
        };
      }),
    );
  }

  // ------------------------------------------------------------ internals

  private openDeadLetterCount(queue: string): number {
    let count = 0;
    for (const [messageId, dlt] of this.deadLettered.get(queue) ?? []) {
      if ((this.redriven.get(`${queue}:${messageId}`) ?? 0) <= dlt.timestamp) count += 1;
    }
    return count;
  }

  private queuesFor(topic: string): string[] {
    return this.bindings.filter((b) => b.topic === topic).map((b) => b.queue);
  }

  private deliveriesOf(message: Omit<Message, 'deliveries'>): Promise<Delivery[]> {
    return Promise.all(this.queuesFor(message.topic).map((queue) => this.deliveryOf(queue, message.topic, message.messageId)));
  }

  private async deliveryOf(queue: string, _topic: string, messageId: string): Promise<Delivery> {
    const binding = this.bindings.find((b) => b.queue === queue)!;
    const delivery = await getJson<Delivery>(`${binding.consumerUrl}/internal/deliveries/${encodeURIComponent(messageId)}`);
    if (!delivery) return pendingDelivery(queue);
    // Redriven but not yet read again by the consumer: pending, as on the in-memory broker.
    const redrivenAt = this.redriven.get(`${queue}:${messageId}`) ?? 0;
    if (delivery.status === 'dead-lettered' && redrivenAt > Date.parse(delivery.deadLetteredAt ?? '')) {
      return { ...delivery, status: 'pending', deadLetterReason: null, deadLetteredAt: null };
    }
    return delivery;
  }

  private messageFromDlt(messageId: string, dlt: IndexedRecord): Omit<Message, 'deliveries'> {
    return {
      ...dlt.message,
      messageId,
      topic: dlt.headers.get('dlt-source-topic') ?? dlt.message.topic,
      partition: Number(dlt.headers.get('dlt-source-partition') ?? -1),
      offset: dlt.headers.get('dlt-source-offset') ?? null,
    };
  }

  private indexRecord(record: KafkaMessage<string, string, string, string>): void {
    this.position.set(`${record.topic}:${record.partition}`, record.offset + 1n);
    const headers = new Map(record.headers);
    const messageId = headers.get('message-id') ?? `${record.topic}-${record.partition}-${record.offset}`;
    const payload = record.value ?? '';
    const indexed: IndexedRecord = {
      message: {
        messageId,
        topic: record.topic,
        payload,
        customerId: customerIdOf(payload),
        correlationId: headers.get('correlation-id') || null,
        publishedAt: new Date(Number(record.timestamp)).toISOString(),
        partition: record.partition,
        offset: String(record.offset),
      },
      headers,
      key: record.key ?? null,
      timestamp: Number(record.timestamp),
    };

    const queue = this.bindings.find((b) => dltTopic(b.queue) === record.topic)?.queue;
    if (queue) {
      this.deadLettered.get(queue)!.set(messageId, indexed);
      return;
    }
    // Keep the first record per message-id; a redrive writes the same id again.
    const existing = this.records.get(messageId);
    if (!existing) this.records.set(messageId, indexed);
    else if (existing.message.partition === null || existing.message.offset === null) {
      existing.message.partition = record.partition;
      existing.message.offset = String(record.offset);
    }
  }

  /** Waits until the index has read every topic up to its end offsets as of now. */
  private async catchUp(topics: string[]): Promise<void> {
    const ends = await this.index.listOffsets({ topics });
    const deadline = Date.now() + CATCH_UP_TIMEOUT_MS;
    for (;;) {
      let behind = false;
      for (const [topic, partitions] of ends) {
        partitions.forEach((end, partition) => {
          if ((this.position.get(`${topic}:${partition}`) ?? 0n) < end) behind = true;
        });
      }
      if (!behind) return;
      if (Date.now() > deadline) throw new Error(`index did not catch up with ${topics.join(', ')}`);
      await sleep(10);
    }
  }
}

function pendingDelivery(queue: string): Delivery {
  return {
    queue,
    status: 'pending',
    attempts: 0,
    deadLetterReason: null,
    lastError: null,
    deliveredAt: null,
    deadLetteredAt: null,
    history: [],
  };
}

async function getJson<T>(url: string): Promise<T | null> {
  try {
    const response = await fetch(url, { signal: AbortSignal.timeout(2000) });
    return response.ok ? ((await response.json()) as T) : null;
  } catch {
    return null;
  }
}

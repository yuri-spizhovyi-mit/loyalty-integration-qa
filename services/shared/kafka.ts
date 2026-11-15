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

/**
 * Kafka plumbing shared by the services when they run with KAFKA_BROKERS set
 * (the in-memory broker is used otherwise). It plays the part of a small
 * in-house platform library: every service produces and consumes the same way.
 *
 * Conventions:
 * - Topics: loyalty.events (engine -> CDP), profile.events (CDP -> notifications).
 * - Key = customerId, so all events of one customer go to one partition and
 *   keep their order. Different customers spread over the partitions.
 * - Headers: message-id (stable across redeliveries and redrives),
 *   correlation-id, and dlt-* on dead-letter records.
 * - One consumer group per queue, named like the in-memory broker's queues
 *   (cdp.loyalty-events, notifications.profile-events). Each group has a
 *   dead-letter topic: `<group>.dlt`.
 */

export const KAFKA_TOPICS = { loyalty: 'loyalty.events', profile: 'profile.events' } as const;
export const dltTopic = (group: string) => `${group}.dlt`;

export function kafkaBrokers(): string[] | null {
  const value = process.env.KAFKA_BROKERS;
  return value ? value.split(',').map((broker) => broker.trim()) : null;
}

export function newMessageId(): string {
  return `msg_${randomUUID()}`;
}

/** Same delivery shape as the in-memory broker, so tests read both the same way. */
export type DeliveryStatus = 'pending' | 'delivered' | 'dead-lettered';
export type DeadLetterReason = 'rejected' | 'max-attempts-exceeded';

export interface Attempt {
  attempt: number;
  at: string;
  /** The handler's status (HTTP-style), or null if it threw. */
  status: number | null;
  outcome: 'ack' | 'reject' | 'retry';
  error: string | null;
}

export interface Delivery {
  queue: string;
  status: DeliveryStatus;
  attempts: number;
  deadLetterReason: DeadLetterReason | null;
  lastError: string | null;
  deliveredAt: string | null;
  deadLetteredAt: string | null;
  history: Attempt[];
  /** Where the record was read from; Kafka only. */
  partition: number;
  offset: string;
}

// ---------------------------------------------------------------- producing

interface Publishable {
  customerId: string;
  correlationId: string;
}

/** What a relay reports at /internal/relay: is it keeping up, and if not, why. */
export interface RelayStatus {
  transport: 'kafka' | 'http' | 'none';
  /** Events accepted from the service but not yet acknowledged by the bus. */
  pending: number;
  /** Failed send attempts since start; each one is retried. */
  failedSends: number;
  lastError: string | null;
  lastErrorAt: string | null;
}

/**
 * Outbox relay to Kafka, the counterpart of the HTTP BrokerRelay: events are
 * queued in memory in publish order and sent one at a time, retrying until
 * Kafka accepts each one. The producer is idempotent with acks=all, so a
 * retry after a lost produce response does not write a second copy.
 */
export class KafkaRelay<T extends Publishable> {
  private readonly producer: Producer<string, string, string, string>;
  private readonly pending: T[] = [];
  private draining = false;
  private failedSends = 0;
  private lastError: { message: string; at: string } | null = null;

  constructor(
    private readonly service: string,
    brokers: string[],
    private readonly topic: string,
    private readonly retryDelayMs = 200,
  ) {
    this.producer = new Producer({
      clientId: `${service}-relay`,
      bootstrapBrokers: brokers,
      serializers: stringSerializers,
      idempotent: true,
      acks: ProduceAcks.ALL,
      autocreateTopics: false,
    });
  }

  publish(event: T): void {
    this.pending.push(event);
    void this.drain();
  }

  status(): RelayStatus {
    return {
      transport: 'kafka',
      pending: this.pending.length,
      failedSends: this.failedSends,
      lastError: this.lastError?.message ?? null,
      lastErrorAt: this.lastError?.at ?? null,
    };
  }

  async close(): Promise<void> {
    await this.producer.close();
  }

  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    try {
      while (this.pending.length > 0) {
        const event = this.pending[0] as T;
        try {
          await this.producer.send({
            messages: [
              {
                topic: this.topic,
                key: event.customerId,
                value: JSON.stringify(event),
                headers: { 'message-id': newMessageId(), 'correlation-id': event.correlationId },
              },
            ],
          });
          this.pending.shift();
        } catch (error) {
          this.failedSends += 1;
          this.lastError = { message: String(error).slice(0, 300), at: new Date().toISOString() };
          console.error(`[${this.service}] Kafka produce failed, retrying: ${String(error)}`);
          await sleep(this.retryDelayMs);
        }
      }
    } finally {
      this.draining = false;
    }
  }
}

// ---------------------------------------------------------------- consuming

/** What a handler answers, in HTTP terms: 2xx ack, 4xx (not 408/429) reject, anything else retry. */
export interface HandlerResult {
  status: number;
  body: unknown;
}

export type MessageHandler = (raw: string, meta: { messageId: string; correlationId: string | null }) => Promise<HandlerResult> | HandlerResult;

/**
 * How a failing record is retried.
 * - blocking: in place; the partition waits (keeps per-key order).
 * - topics: moved to `<group>.retry-N` with a delay, so the partition keeps
 *   moving; a later record of the same key can overtake it.
 */
export type RetryMode = 'blocking' | 'topics';

export function retryModeFromEnv(): RetryMode {
  return process.env.KAFKA_RETRY_MODE === 'topics' ? 'topics' : 'blocking';
}

/** Retry topic for the Nth retry (after attempt N failed). */
export const retryTopic = (group: string, n: number) => `${group}.retry-${n}`;

export interface ConsumerRunnerOptions {
  service: string;
  brokers: string[];
  group: string;
  topic: string;
  handler: MessageHandler;
  maxAttempts: number;
  retryBaseMs: number;
  retryMode: RetryMode;
  /**
   * Static group membership (KIP-345): a restarted instance with the same id
   * gets its partitions back at once, without waiting for the session timeout.
   */
  instanceId?: string;
}

/** Where a record was first read from; kept through retry topics so the DLT can point at it. */
interface Origin {
  topic: string;
  partition: number;
  offset: string;
}

/**
 * Consumes one topic as one consumer group, with the same rules as the
 * in-memory broker: retries with exponential backoff (100, 200, 400 ms by
 * default), then the dead-letter topic; a permanent rejection goes there at
 * once. Two retry modes (see RetryMode):
 *
 * - blocking: a partition does not move on until its current record is
 *   acknowledged or dead-lettered. Per-customer order is kept, at the price
 *   of head-of-line blocking for the other customers on that partition.
 * - topics: a failing record is produced to `<group>.retry-N` and the
 *   source offset is committed, so the partition keeps moving. Each retry
 *   topic has one fixed delay, so records on it become due in the order they
 *   arrive; the consumer waits until a record is due. A later event of the
 *   same customer can overtake a retried one, which the consumer must
 *   tolerate (the CDP does, with sequence versioning).
 *
 * In both modes an offset is committed only after the record is handled:
 * acked, dead-lettered, or safely parked on a retry topic. A crash in
 * between means redelivery, not loss.
 */
export class ConsumerRunner {
  private consumer: Consumer<string, string, string, string>;
  private readonly producer: Producer<string, string, string, string>;
  private readonly deliveries = new Map<string, Delivery>();
  private readonly chains = new Map<string, Promise<void>>();
  /** Committed offset (next to read) per main-topic partition, after the commit succeeded. */
  private readonly committed = new Map<number, bigint>();
  private assigned: number[] = [];
  private joined = false;
  private consuming = false;
  private closing = false;
  private stream: { destroy(error?: Error): void } | null = null;
  private restarts = 0;

  constructor(private readonly options: ConsumerRunnerOptions) {
    this.consumer = this.createConsumer();
    this.producer = new Producer({
      clientId: `${options.service}-retry-dlt`,
      bootstrapBrokers: options.brokers,
      serializers: stringSerializers,
      idempotent: true,
      acks: ProduceAcks.ALL,
      autocreateTopics: false,
    });
  }

  private createConsumer(): Consumer<string, string, string, string> {
    const { options } = this;
    const consumer = new Consumer<string, string, string, string>({
      clientId: `${options.service}-consumer`,
      groupId: options.group,
      ...(options.instanceId ? { groupInstanceId: options.instanceId } : {}),
      bootstrapBrokers: options.brokers,
      deserializers: stringDeserializers,
      autocommit: false,
      sessionTimeout: 10_000,
      heartbeatInterval: 1_000,
      maxWaitTime: 100,
    });
    consumer.on('consumer:group:join', (payload) => {
      this.joined = true;
      this.assigned = (payload.assignments ?? [])
        .filter((a) => a.topic === options.topic)
        .flatMap((a) => a.partitions)
        .sort((a, b) => a - b);
      console.log(
        `[${options.service}] joined consumer group ${options.group} on ${this.topics.join(', ')} (retries: ${options.retryMode})`,
      );
    });
    return consumer;
  }

  /** True while the group is joined and the stream is reading, so /health can report it. */
  get ready(): boolean {
    return this.joined && this.consuming;
  }

  get retryMode(): RetryMode {
    return this.options.retryMode;
  }

  /** How many times the supervisor has restarted a failed or closed stream. */
  get streamRestarts(): number {
    return this.restarts;
  }

  /** Test only: break the current stream, as a client library giving up would. */
  failStream(): boolean {
    if (!this.stream) return false;
    this.stream.destroy(new Error('injected stream failure'));
    return true;
  }

  /** Main-topic partitions this instance owns since the last rebalance. */
  get assignedPartitions(): number[] {
    return [...this.assigned];
  }

  private get retryTopics(): string[] {
    if (this.options.retryMode !== 'topics') return [];
    return Array.from({ length: this.options.maxAttempts - 1 }, (_, i) => retryTopic(this.options.group, i + 1));
  }

  private get topics(): string[] {
    return [this.options.topic, ...this.retryTopics];
  }

  delivery(messageId: string): Delivery | undefined {
    const delivery = this.deliveries.get(messageId);
    return delivery ? structuredClone(delivery) : undefined;
  }

  /** Records on the main topic not yet committed by this group, per partition. */
  async lag(): Promise<number[]> {
    const lag = await this.consumer.getLag({ topics: [this.options.topic] });
    return (lag.get(this.options.topic) ?? []).map((value) => Number(value < 0n ? 0n : value));
  }

  /** Committed offset per main-topic partition (the next offset the group will read), as strings. */
  committedOffsets(): Record<string, string> {
    return Object.fromEntries([...this.committed].map(([partition, offset]) => [String(partition), String(offset)]));
  }

  /** Starts consuming, and keeps doing so: see supervise(). */
  async start(): Promise<void> {
    void this.supervise();
  }

  /**
   * Keeps a stream running. If the stream fails or closes (for example after
   * a long Kafka outage the client gives up fetching), the consumer is closed
   * and a new one joins the group and subscribes again. Without this, the
   * consumer stays in the group with no partitions and silently stops
   * reading, which is the bug the chaos tests found. Records that were in
   * flight are not committed, so the new consumer reads them again and they
   * are handled as duplicates.
   */
  private async supervise(): Promise<void> {
    while (!this.closing) {
      const consumer = this.consumer;
      try {
        const stream = await consumer.consume({ topics: this.topics, mode: 'committed', fallbackMode: 'earliest' });
        this.stream = stream;
        this.consuming = true;
        await new Promise<void>((resolve) => {
          stream.on('data', (message) => this.enqueue(message));
          stream.once('error', (error) => {
            console.error(`[${this.options.service}] consumer stream failed, restarting: ${String(error)}`);
            resolve();
          });
          stream.once('close', () => resolve());
        });
      } catch (error) {
        console.error(`[${this.options.service}] cannot start consumer yet: ${String(error)}`);
      }
      this.stream = null;
      this.consuming = false;
      this.joined = false;
      this.assigned = [];
      if (this.closing) return;
      this.restarts += 1;
      await consumer.close(true).catch(() => undefined);
      await sleep(1000);
      this.consumer = this.createConsumer();
    }
  }

  async close(): Promise<void> {
    this.closing = true;
    await this.consumer.close(true);
    await this.producer.close();
  }

  /** One promise chain per topic partition: in order within it, in parallel across them. */
  private enqueue(message: KafkaMessage<string, string, string, string>): void {
    const lane = `${message.topic}:${message.partition}`;
    const previous = this.chains.get(lane) ?? Promise.resolve();
    const next = previous
      .then(() => this.process(message))
      .catch((error) => console.error(`[${this.options.service}] processing failed: ${String(error)}`));
    this.chains.set(lane, next);
  }

  private async process(message: KafkaMessage<string, string, string, string>): Promise<void> {
    const { group, maxAttempts, retryBaseMs, retryMode } = this.options;
    const headers = message.headers;
    const messageId = headers.get('message-id') ?? `${message.topic}-${message.partition}-${message.offset}`;
    const fromRetryTopic = message.topic !== this.options.topic;
    const origin: Origin = fromRetryTopic
      ? {
          topic: headers.get('origin-topic') ?? this.options.topic,
          partition: Number(headers.get('origin-partition') ?? -1),
          offset: headers.get('origin-offset') ?? '-1',
        }
      : { topic: message.topic, partition: message.partition, offset: String(message.offset) };

    // A record from the main topic starts an attempt budget: either new, or a
    // redrive (same message-id), which keeps the history. A record from a
    // retry topic continues the budget it carries.
    const existing = this.deliveries.get(messageId);
    const delivery: Delivery =
      existing ??
      ({
        queue: group,
        status: 'pending',
        attempts: 0,
        deadLetterReason: null,
        lastError: null,
        deliveredAt: null,
        deadLetteredAt: null,
        history: [],
        partition: origin.partition,
        offset: origin.offset,
      } satisfies Delivery);
    if (!fromRetryTopic) {
      Object.assign(delivery, { status: 'pending', attempts: 0, deadLetterReason: null, deadLetteredAt: null });
    } else {
      delivery.attempts = Number(headers.get('retry-attempt') ?? delivery.attempts + 1) - 1;
      // Wait until the record is due. Records on one retry topic share one delay, so they are due in order.
      // Loop: a timer can fire a millisecond early by Date.now(), and the
      // record must never be retried before it is due.
      const due = Number(headers.get('retry-due') ?? 0);
      while (Date.now() < due) await sleep(due - Date.now());
    }
    delivery.partition = origin.partition;
    delivery.offset = origin.offset;
    this.deliveries.set(messageId, delivery);

    while (delivery.status === 'pending') {
      delivery.attempts += 1;
      const attempt = await this.attempt(message.value ?? '', messageId, headers.get('correlation-id') ?? null, delivery.attempts);
      delivery.history.push(attempt);
      delivery.lastError = attempt.error;

      if (attempt.outcome === 'ack') {
        delivery.status = 'delivered';
        delivery.deliveredAt = attempt.at;
      } else if (attempt.outcome === 'reject') {
        await this.deadLetter(message, messageId, origin, delivery, 'rejected');
      } else if (delivery.attempts >= maxAttempts) {
        await this.deadLetter(message, messageId, origin, delivery, 'max-attempts-exceeded');
      } else if (retryMode === 'topics') {
        // Park it and let the partition move on.
        const n = delivery.attempts;
        await this.produce(retryTopic(group, n), message, {
          'message-id': messageId,
          'correlation-id': headers.get('correlation-id') ?? '',
          'retry-attempt': String(n + 1),
          'retry-due': String(Date.now() + retryBaseMs * 2 ** (n - 1)),
          'origin-topic': origin.topic,
          'origin-partition': String(origin.partition),
          'origin-offset': origin.offset,
        });
        break;
      } else {
        const due = Date.now() + retryBaseMs * 2 ** (delivery.attempts - 1);
        while (Date.now() < due) await sleep(due - Date.now());
      }
    }
    // Commit only now: the record is acked, in the DLT, or parked on a retry topic.
    await message.commit();
    if (!fromRetryTopic) this.committed.set(message.partition, message.offset + 1n);
  }

  private async attempt(raw: string, messageId: string, correlationId: string | null, attempt: number): Promise<Attempt> {
    const at = new Date().toISOString();
    try {
      const { status, body } = await this.options.handler(raw, { messageId, correlationId });
      if (status >= 200 && status < 300) return { attempt, at, status, outcome: 'ack', error: null };
      const error = `status ${status}: ${JSON.stringify(body).slice(0, 500)}`;
      const permanent = status >= 400 && status < 500 && status !== 408 && status !== 429;
      return { attempt, at, status, outcome: permanent ? 'reject' : 'retry', error };
    } catch (error) {
      return { attempt, at, status: null, outcome: 'retry', error: String(error) };
    }
  }

  private async deadLetter(
    message: KafkaMessage<string, string, string, string>,
    messageId: string,
    origin: Origin,
    delivery: Delivery,
    reason: DeadLetterReason,
  ): Promise<void> {
    // The DLT record keeps the original key and bytes, plus why it failed and where it came from.
    await this.produce(dltTopic(this.options.group), message, {
      'message-id': messageId,
      'correlation-id': message.headers.get('correlation-id') ?? '',
      'dlt-reason': reason,
      'dlt-attempts': String(delivery.attempts),
      'dlt-error': (delivery.lastError ?? '').slice(0, 500),
      'dlt-source-topic': origin.topic,
      'dlt-source-partition': String(origin.partition),
      'dlt-source-offset': origin.offset,
    });
    delivery.status = 'dead-lettered';
    delivery.deadLetterReason = reason;
    delivery.deadLetteredAt = new Date().toISOString();
    console.warn(`[${this.options.service}] dead-lettered ${messageId} (${reason}): ${delivery.lastError}`);
  }

  /** Produces a copy of the record (same key and bytes) with the given headers, retrying until Kafka accepts it. */
  private async produce(
    topic: string,
    message: KafkaMessage<string, string, string, string>,
    headers: Record<string, string>,
  ): Promise<void> {
    for (;;) {
      try {
        await this.producer.send({
          messages: [{ topic, key: message.key ?? undefined, value: message.value ?? '', headers }],
        });
        return;
      } catch (error) {
        console.error(`[${this.options.service}] produce to ${topic} failed, retrying: ${String(error)}`);
        await sleep(200);
      }
    }
  }
}

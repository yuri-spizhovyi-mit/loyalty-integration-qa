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

export interface ConsumerRunnerOptions {
  service: string;
  brokers: string[];
  group: string;
  topic: string;
  handler: MessageHandler;
  maxAttempts: number;
  retryBaseMs: number;
}

/**
 * Consumes one topic as one consumer group, with the same rules as the
 * in-memory broker: retries with exponential backoff, then the dead-letter
 * topic; a permanent rejection goes there at once.
 *
 * Retries are blocking: a partition does not move on until its current
 * record is acknowledged or dead-lettered. That keeps per-customer order
 * (a later event never overtakes a retried one), at the price of
 * head-of-line blocking for the other customers on that partition. Records
 * on other partitions are unaffected. The tests in tests/kafka pin this down.
 *
 * Offsets are committed only after a record is fully handled (acked or
 * written to the DLT), so a crash in between means redelivery, not loss.
 */
export class ConsumerRunner {
  private readonly consumer: Consumer<string, string, string, string>;
  private readonly dltProducer: Producer<string, string, string, string>;
  private readonly deliveries = new Map<string, Delivery>();
  private readonly chains = new Map<number, Promise<void>>();
  /** Last committed offset (next to read) per partition, after the commit succeeded. */
  private readonly committed = new Map<number, bigint>();
  private joined = false;

  constructor(private readonly options: ConsumerRunnerOptions) {
    this.consumer = new Consumer({
      clientId: `${options.service}-consumer`,
      groupId: options.group,
      bootstrapBrokers: options.brokers,
      deserializers: stringDeserializers,
      autocommit: false,
      sessionTimeout: 10_000,
      heartbeatInterval: 1_000,
      maxWaitTime: 100,
    });
    this.dltProducer = new Producer({
      clientId: `${options.service}-dlt`,
      bootstrapBrokers: options.brokers,
      serializers: stringSerializers,
      idempotent: true,
      acks: ProduceAcks.ALL,
      autocreateTopics: false,
    });
    this.consumer.on('consumer:group:join', () => {
      this.joined = true;
      console.log(`[${options.service}] joined consumer group ${options.group} on ${options.topic}`);
    });
  }

  /** True once the group is joined, so /health can wait for it. */
  get ready(): boolean {
    return this.joined;
  }

  delivery(messageId: string): Delivery | undefined {
    const delivery = this.deliveries.get(messageId);
    return delivery ? structuredClone(delivery) : undefined;
  }

  /** Records not yet consumed by this group, per partition. */
  async lag(): Promise<number[]> {
    const lag = await this.consumer.getLag({ topics: [this.options.topic] });
    return (lag.get(this.options.topic) ?? []).map((value) => Number(value < 0n ? 0n : value));
  }

  /** Committed offset per partition (the next offset the group will read), as strings. */
  committedOffsets(): Record<string, string> {
    return Object.fromEntries([...this.committed].map(([partition, offset]) => [String(partition), String(offset)]));
  }

  async start(): Promise<void> {
    // Keep trying until Kafka is reachable and the topic exists.
    for (;;) {
      try {
        const stream = await this.consumer.consume({
          topics: [this.options.topic],
          mode: 'committed',
          fallbackMode: 'earliest',
        });
        stream.on('data', (message) => this.enqueue(message));
        stream.on('error', (error) => console.error(`[${this.options.service}] consumer error: ${String(error)}`));
        return;
      } catch (error) {
        console.error(`[${this.options.service}] cannot start consumer yet: ${String(error)}`);
        await sleep(1000);
      }
    }
  }

  async close(): Promise<void> {
    await this.consumer.close(true);
    await this.dltProducer.close();
  }

  /** One promise chain per partition: in order within a partition, in parallel across partitions. */
  private enqueue(message: KafkaMessage<string, string, string, string>): void {
    const previous = this.chains.get(message.partition) ?? Promise.resolve();
    const next = previous
      .then(() => this.process(message))
      .catch((error) => console.error(`[${this.options.service}] processing failed: ${String(error)}`));
    this.chains.set(message.partition, next);
  }

  private async process(message: KafkaMessage<string, string, string, string>): Promise<void> {
    const { group, handler, maxAttempts, retryBaseMs } = this.options;
    const messageId = message.headers.get('message-id') ?? `${message.topic}-${message.partition}-${message.offset}`;
    const correlationId = message.headers.get('correlation-id') ?? null;
    const raw = message.value ?? '';

    // A redriven record keeps its message-id: start a fresh attempt budget but keep the history.
    const existing = this.deliveries.get(messageId);
    const delivery: Delivery = existing
      ? Object.assign(existing, { status: 'pending', attempts: 0, deadLetterReason: null, deadLetteredAt: null })
      : {
          queue: group,
          status: 'pending',
          attempts: 0,
          deadLetterReason: null,
          lastError: null,
          deliveredAt: null,
          deadLetteredAt: null,
          history: [],
          partition: message.partition,
          offset: String(message.offset),
        };
    delivery.partition = message.partition;
    delivery.offset = String(message.offset);
    this.deliveries.set(messageId, delivery);

    while (delivery.status === 'pending') {
      delivery.attempts += 1;
      const attempt = await this.attempt(handler, raw, messageId, correlationId, delivery.attempts);
      delivery.history.push(attempt);
      delivery.lastError = attempt.error;

      if (attempt.outcome === 'ack') {
        delivery.status = 'delivered';
        delivery.deliveredAt = attempt.at;
      } else if (attempt.outcome === 'reject') {
        await this.deadLetter(message, messageId, delivery, 'rejected');
      } else if (delivery.attempts >= maxAttempts) {
        await this.deadLetter(message, messageId, delivery, 'max-attempts-exceeded');
      } else {
        await sleep(retryBaseMs * 2 ** (delivery.attempts - 1));
      }
    }
    // Commit only now: the record is acked or safely in the DLT.
    await message.commit();
    this.committed.set(message.partition, message.offset + 1n);
  }

  private async attempt(
    handler: MessageHandler,
    raw: string,
    messageId: string,
    correlationId: string | null,
    attempt: number,
  ): Promise<Attempt> {
    const at = new Date().toISOString();
    try {
      const { status, body } = await handler(raw, { messageId, correlationId });
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
    delivery: Delivery,
    reason: DeadLetterReason,
  ): Promise<void> {
    // The DLT record keeps the original key and bytes, plus why and where it failed.
    for (;;) {
      try {
        await this.dltProducer.send({
          messages: [
            {
              topic: dltTopic(this.options.group),
              key: message.key ?? undefined,
              value: message.value ?? '',
              headers: {
                'message-id': messageId,
                'correlation-id': message.headers.get('correlation-id') ?? '',
                'dlt-reason': reason,
                'dlt-attempts': String(delivery.attempts),
                'dlt-error': (delivery.lastError ?? '').slice(0, 500),
                'dlt-source-topic': message.topic,
                'dlt-source-partition': String(message.partition),
                'dlt-source-offset': String(message.offset),
              },
            },
          ],
        });
        break;
      } catch (error) {
        console.error(`[${this.options.service}] DLT produce failed, retrying: ${String(error)}`);
        await sleep(200);
      }
    }
    delivery.status = 'dead-lettered';
    delivery.deadLetterReason = reason;
    delivery.deadLetteredAt = new Date().toISOString();
    console.warn(`[${this.options.service}] dead-lettered ${messageId} (${reason}): ${delivery.lastError}`);
  }
}

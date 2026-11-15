import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';

/**
 * A small in-memory message broker with the delivery semantics the tests
 * care about:
 *
 * - Topics fan out to queues; each queue has one push subscriber (an HTTP URL).
 * - At-least-once delivery: a message is retried until the subscriber answers
 *   2xx. Consumers must therefore deduplicate.
 * - No ordering guarantee: every message is delivered on its own, so a retried
 *   message can arrive after later ones. Consumers must use `sequence`.
 * - Dead-letter queue per queue: a 4xx answer means the consumer will never
 *   accept the message (malformed, invalid), so it is dead-lettered at once;
 *   5xx, timeouts and connection errors are retried with exponential backoff
 *   and dead-lettered after `maxAttempts`.
 */

export interface BrokerOptions {
  maxAttempts: number;
  retryBaseMs: number;
  deliveryTimeoutMs: number;
}

export interface QueueBinding {
  queue: string;
  topic: string;
  subscriberUrl: string;
}

export type DeliveryStatus = 'pending' | 'delivered' | 'dead-lettered';
export type DeadLetterReason = 'rejected' | 'max-attempts-exceeded';

export interface Attempt {
  attempt: number;
  at: string;
  /** HTTP status from the subscriber, or null when it could not be reached. */
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
}

export interface Message {
  messageId: string;
  topic: string;
  /** Exactly what the publisher sent; it may not be valid JSON. */
  payload: string;
  /** Read from the payload when it is JSON with a `customerId`, for filtering. */
  customerId: string | null;
  correlationId: string | null;
  publishedAt: string;
  deliveries: Delivery[];
}

const ERROR_BODY_CHARS = 500;

function customerIdOf(payload: string): string | null {
  try {
    const parsed = JSON.parse(payload) as { customerId?: unknown };
    return typeof parsed?.customerId === 'string' ? parsed.customerId : null;
  } catch {
    return null;
  }
}

export class Broker {
  private readonly messages = new Map<string, Message>();

  constructor(
    private readonly bindings: QueueBinding[],
    private readonly options: BrokerOptions,
  ) {}

  queues(): QueueBinding[] {
    return this.bindings;
  }

  hasTopic(topic: string): boolean {
    return this.bindings.some((binding) => binding.topic === topic);
  }

  hasQueue(queue: string): boolean {
    return this.bindings.some((binding) => binding.queue === queue);
  }

  publish(topic: string, payload: string, correlationId: string | null): Message {
    const message: Message = {
      messageId: `msg_${randomUUID()}`,
      topic,
      payload,
      customerId: customerIdOf(payload),
      correlationId,
      publishedAt: new Date().toISOString(),
      deliveries: this.bindings
        .filter((binding) => binding.topic === topic)
        .map((binding) => ({
          queue: binding.queue,
          status: 'pending',
          attempts: 0,
          deadLetterReason: null,
          lastError: null,
          deliveredAt: null,
          deadLetteredAt: null,
          history: [],
        })),
    };
    this.messages.set(message.messageId, message);
    for (const delivery of message.deliveries) void this.deliver(message, delivery);
    return message;
  }

  getMessage(messageId: string): Message | undefined {
    return this.messages.get(messageId);
  }

  deadLetters(queue: string, filter: { customerId?: string; messageId?: string }): (Message & { delivery: Delivery })[] {
    const result: (Message & { delivery: Delivery })[] = [];
    for (const message of this.messages.values()) {
      if (filter.customerId && message.customerId !== filter.customerId) continue;
      if (filter.messageId && message.messageId !== filter.messageId) continue;
      const delivery = message.deliveries.find((d) => d.queue === queue && d.status === 'dead-lettered');
      if (delivery) result.push({ ...message, delivery });
    }
    return result;
  }

  /** Moves a dead-lettered message back to the queue with a fresh attempt budget. */
  redrive(queue: string, messageId: string): Message | undefined {
    const message = this.messages.get(messageId);
    const delivery = message?.deliveries.find((d) => d.queue === queue && d.status === 'dead-lettered');
    if (!message || !delivery) return undefined;
    Object.assign(delivery, {
      status: 'pending',
      attempts: 0,
      deadLetterReason: null,
      deadLetteredAt: null,
    } satisfies Partial<Delivery>);
    void this.deliver(message, delivery);
    return message;
  }

  stats() {
    return this.bindings.map(({ queue, topic, subscriberUrl }) => {
      const counts = { pending: 0, delivered: 0, 'dead-lettered': 0 };
      for (const message of this.messages.values()) {
        const delivery = message.deliveries.find((d) => d.queue === queue);
        if (delivery) counts[delivery.status] += 1;
      }
      return { queue, topic, subscriberUrl, ...counts };
    });
  }

  private async deliver(message: Message, delivery: Delivery): Promise<void> {
    const binding = this.bindings.find((b) => b.queue === delivery.queue);
    if (!binding) return;

    while (delivery.status === 'pending') {
      delivery.attempts += 1;
      const attempt = await this.attempt(binding.subscriberUrl, message, delivery.attempts);
      delivery.history.push(attempt);
      delivery.lastError = attempt.error;

      if (attempt.outcome === 'ack') {
        delivery.status = 'delivered';
        delivery.deliveredAt = attempt.at;
      } else if (attempt.outcome === 'reject') {
        this.deadLetter(message, delivery, 'rejected');
      } else if (delivery.attempts >= this.options.maxAttempts) {
        this.deadLetter(message, delivery, 'max-attempts-exceeded');
      } else {
        await sleep(this.options.retryBaseMs * 2 ** (delivery.attempts - 1));
      }
    }
  }

  private deadLetter(message: Message, delivery: Delivery, reason: DeadLetterReason): void {
    delivery.status = 'dead-lettered';
    delivery.deadLetterReason = reason;
    delivery.deadLetteredAt = new Date().toISOString();
    console.warn(
      `[broker] dead-lettered ${message.messageId} on ${delivery.queue} (${reason}) ` +
        `customerId=${message.customerId ?? '-'} correlationId=${message.correlationId ?? '-'}: ${delivery.lastError}`,
    );
  }

  private async attempt(url: string, message: Message, attempt: number): Promise<Attempt> {
    const at = new Date().toISOString();
    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Message-Id': message.messageId,
          'X-Delivery-Attempt': String(attempt),
          ...(message.correlationId ? { 'X-Correlation-ID': message.correlationId } : {}),
        },
        body: message.payload,
        signal: AbortSignal.timeout(this.options.deliveryTimeoutMs),
      });
      const status = response.status;
      if (status >= 200 && status < 300) return { attempt, at, status, outcome: 'ack', error: null };
      const error = `HTTP ${status}: ${(await response.text()).slice(0, ERROR_BODY_CHARS)}`;
      // 408 and 429 are "try again later", not "this message is bad".
      const permanent = status >= 400 && status < 500 && status !== 408 && status !== 429;
      return { attempt, at, status, outcome: permanent ? 'reject' : 'retry', error };
    } catch (error) {
      return { attempt, at, status: null, outcome: 'retry', error: String(error) };
    }
  }
}

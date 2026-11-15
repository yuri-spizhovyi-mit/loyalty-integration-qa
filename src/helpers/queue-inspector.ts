import type { APIRequestContext } from '@playwright/test';
import { CDP_QUEUE, LOYALTY_TOPIC } from '../config.js';

export type DeliveryStatus = 'pending' | 'delivered' | 'dead-lettered';

export interface DeliveryAttempt {
  attempt: number;
  at: string;
  status: number | null;
  outcome: 'ack' | 'reject' | 'retry';
  error: string | null;
}

export interface Delivery {
  queue: string;
  status: DeliveryStatus;
  attempts: number;
  deadLetterReason: 'rejected' | 'max-attempts-exceeded' | null;
  lastError: string | null;
  deliveredAt: string | null;
  deadLetteredAt: string | null;
  history: DeliveryAttempt[];
}

export interface BrokerMessage {
  messageId: string;
  topic: string;
  payload: string;
  customerId: string | null;
  correlationId: string | null;
  publishedAt: string;
  deliveries: Delivery[];
}

export interface DeadLetter extends BrokerMessage {
  delivery: Delivery;
}

/**
 * Test-side view of the broker: publish messages directly (valid, duplicate,
 * reordered or malformed) and inspect deliveries and the dead-letter queue.
 * The broker is test infrastructure, so its responses are not contract-checked.
 */
export class QueueInspector {
  constructor(private readonly request: APIRequestContext) {}

  /** Publishes an event object, or a raw string exactly as given. Returns the broker's messageId. */
  async publish(message: unknown, topic = LOYALTY_TOPIC): Promise<string> {
    const body = typeof message === 'string' ? message : JSON.stringify(message);
    const response = await this.request.post(`/topics/${topic}/messages`, {
      // A Buffer so malformed payloads are sent byte for byte (Playwright
      // JSON-encodes a string that is not valid JSON).
      data: Buffer.from(body),
      headers: { 'Content-Type': 'application/json' },
    });
    if (response.status() !== 202) {
      throw new Error(`broker refused message on ${topic}: ${response.status()} ${await response.text()}`);
    }
    return ((await response.json()) as { messageId: string }).messageId;
  }

  /** Publishes several messages one after the other, in the given order. */
  async publishAll(messages: unknown[], topic = LOYALTY_TOPIC): Promise<string[]> {
    const ids: string[] = [];
    for (const message of messages) ids.push(await this.publish(message, topic));
    return ids;
  }

  /** Messages published on a topic for one customer, oldest first. */
  async messagesOn(topic: string, customerId: string): Promise<BrokerMessage[]> {
    const response = await this.request.get(`/topics/${topic}/messages`, { params: { customerId } });
    if (response.status() !== 200) throw new Error(`messages on ${topic}: ${response.status()} ${await response.text()}`);
    return ((await response.json()) as { messages: BrokerMessage[] }).messages;
  }

  async message(messageId: string): Promise<BrokerMessage> {
    const response = await this.request.get(`/messages/${messageId}`);
    if (response.status() !== 200) throw new Error(`message ${messageId}: ${response.status()} ${await response.text()}`);
    return (await response.json()) as BrokerMessage;
  }

  /** The delivery of one message to one queue. Poll it until it leaves "pending". */
  async delivery(messageId: string, queue = CDP_QUEUE): Promise<Delivery> {
    const delivery = (await this.message(messageId)).deliveries.find((d) => d.queue === queue);
    if (!delivery) throw new Error(`message ${messageId} has no delivery to ${queue}`);
    return delivery;
  }

  async deadLetters(filter: { customerId?: string; messageId?: string }, queue = CDP_QUEUE): Promise<DeadLetter[]> {
    const params: Record<string, string> = {};
    if (filter.customerId) params.customerId = filter.customerId;
    if (filter.messageId) params.messageId = filter.messageId;
    const response = await this.request.get(`/queues/${queue}/dead-letters`, { params });
    if (response.status() !== 200) throw new Error(`dead letters for ${queue}: ${response.status()}`);
    return ((await response.json()) as { messages: DeadLetter[] }).messages;
  }

  async redrive(messageId: string, queue = CDP_QUEUE): Promise<void> {
    const response = await this.request.post(`/queues/${queue}/dead-letters/${messageId}/redrive`);
    if (response.status() !== 202) throw new Error(`redrive ${messageId}: ${response.status()} ${await response.text()}`);
  }
}

import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';

export type LoyaltyEventType =
  | 'customer.created'
  | 'points.earned'
  | 'points.redeemed'
  | 'points.refunded'
  | 'tier.changed';

export interface LoyaltyEvent<T = Record<string, unknown>> {
  eventId: string;
  type: LoyaltyEventType;
  /** Increases by one per customer, so a consumer can detect gaps and out-of-order delivery. */
  sequence: number;
  customerId: string;
  correlationId: string;
  occurredAt: string;
  data: T;
}

/** Where the engine sends its events. The engine only depends on this interface. */
export interface EventPublisher {
  publish(event: LoyaltyEvent): void;
}

/** Every published event, kept so tests can inspect them through /internal/outbox. */
export class InMemoryEventLog implements EventPublisher {
  readonly events: LoyaltyEvent[] = [];
  publish(event: LoyaltyEvent): void {
    this.events.push(event);
  }
  forCustomer(customerId: string): LoyaltyEvent[] {
    return this.events.filter((event) => event.customerId === customerId);
  }
}

/** Sends each event to every publisher, e.g. the outbox log and the broker relay. */
export class FanOutPublisher implements EventPublisher {
  private readonly publishers: EventPublisher[];
  constructor(...publishers: EventPublisher[]) {
    this.publishers = publishers;
  }
  publish(event: LoyaltyEvent): void {
    for (const publisher of this.publishers) publisher.publish(event);
  }
}

/**
 * Outbox relay: forwards events to the broker in publish order, one at a
 * time, and retries until the broker accepts each one. The HTTP request that
 * caused the event never waits for this, so a broker outage delays events
 * instead of failing ledger writes or losing events.
 */
export class BrokerRelay implements EventPublisher {
  private readonly pending: LoyaltyEvent[] = [];
  private draining = false;
  private failedSends = 0;
  private lastError: { message: string; at: string } | null = null;

  constructor(
    private readonly brokerUrl: string,
    private readonly topic = 'loyalty.events',
    private readonly retryDelayMs = 200,
  ) {}

  publish(event: LoyaltyEvent): void {
    this.pending.push(event);
    void this.drain();
  }

  status() {
    return {
      transport: 'http' as const,
      pending: this.pending.length,
      failedSends: this.failedSends,
      lastError: this.lastError?.message ?? null,
      lastErrorAt: this.lastError?.at ?? null,
    };
  }

  private failed(message: string): void {
    this.failedSends += 1;
    this.lastError = { message: message.slice(0, 300), at: new Date().toISOString() };
    console.error(`[loyalty-engine] ${message}`);
  }

  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    try {
      while (this.pending.length > 0) {
        const event = this.pending[0] as LoyaltyEvent;
        if (await this.send(event)) this.pending.shift();
        else await sleep(this.retryDelayMs);
      }
    } finally {
      this.draining = false;
    }
  }

  private async send(event: LoyaltyEvent): Promise<boolean> {
    try {
      const response = await fetch(`${this.brokerUrl}/topics/${this.topic}/messages`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Correlation-ID': event.correlationId },
        body: JSON.stringify(event),
        signal: AbortSignal.timeout(2000),
      });
      if (response.ok) return true;
      this.failed(`broker refused event ${event.eventId}: ${response.status}`);
    } catch (error) {
      this.failed(`broker unreachable for event ${event.eventId}: ${String(error)}`);
    }
    return false;
  }
}

export function buildEvent<T extends Record<string, unknown>>(
  fields: Omit<LoyaltyEvent<T>, 'eventId' | 'occurredAt'>,
): LoyaltyEvent<T> {
  return { eventId: randomUUID(), occurredAt: new Date().toISOString(), ...fields };
}

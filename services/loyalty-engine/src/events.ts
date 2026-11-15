import { randomUUID } from 'node:crypto';

export type LoyaltyEventType = 'points.earned' | 'points.redeemed' | 'points.refunded' | 'tier.changed';

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

/**
 * Where the engine sends its events. Step 4 replaces the in-memory log
 * with a real queue; the engine only depends on this interface.
 */
export interface EventPublisher {
  publish(event: LoyaltyEvent): void;
}

export class InMemoryEventLog implements EventPublisher {
  readonly events: LoyaltyEvent[] = [];
  publish(event: LoyaltyEvent): void {
    this.events.push(event);
  }
}

export function buildEvent<T extends Record<string, unknown>>(
  fields: Omit<LoyaltyEvent<T>, 'eventId' | 'occurredAt'>,
): LoyaltyEvent<T> {
  return { eventId: randomUUID(), occurredAt: new Date().toISOString(), ...fields };
}

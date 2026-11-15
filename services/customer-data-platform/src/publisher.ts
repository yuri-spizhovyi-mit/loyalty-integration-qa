import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import type { LoyaltyEvent } from './events.js';

/**
 * Events the customer data platform publishes on `profile.events`
 * (described in cdp-asyncapi.yaml). Tier names use the profile's lower-case
 * vocabulary, not the engine's.
 */
export interface ProfileTierChanged {
  eventId: string;
  type: 'profile.tier_changed';
  customerId: string;
  correlationId: string;
  occurredAt: string;
  /** eventId of the engine's tier.changed event this one comes from. */
  sourceEventId: string;
  data: {
    previousTier: 'bronze' | 'silver' | 'gold';
    newTier: 'bronze' | 'silver' | 'gold';
    direction: 'upgrade' | 'downgrade';
    /** As the profile knows it when the tier changes; null if customer.created has not arrived yet. */
    marketingConsent: boolean | null;
    transactionId: string;
  };
}

export interface ProfileEventPublisher {
  publish(event: ProfileTierChanged): void;
}

export function profileTierChanged(
  source: Extract<LoyaltyEvent, { type: 'tier.changed' }>,
  marketingConsent: boolean | null,
): ProfileTierChanged {
  const lower = (tier: string) => tier.toLowerCase() as ProfileTierChanged['data']['newTier'];
  return {
    eventId: randomUUID(),
    type: 'profile.tier_changed',
    customerId: source.customerId,
    correlationId: source.correlationId,
    occurredAt: new Date().toISOString(),
    sourceEventId: source.eventId,
    data: {
      previousTier: lower(source.data.previousTier),
      newTier: lower(source.data.newTier),
      direction: source.data.direction,
      marketingConsent,
      transactionId: source.data.transactionId,
    },
  };
}

/** Used when no broker is configured: events are logged and dropped. */
export class NoopPublisher implements ProfileEventPublisher {
  publish(event: ProfileTierChanged): void {
    console.log(`[cdp] no broker configured; dropping ${event.type} for ${event.customerId}`);
  }
}

/**
 * Outbox relay to the broker, the same pattern as the engine's: the event is
 * queued in the same step that writes the profile, then sent in order with
 * retries. A failure after the profile write (for example a lost ack to the
 * broker) therefore cannot lose the event, and the redelivered tier.changed
 * is a duplicate that publishes nothing new.
 */
export class BrokerRelay implements ProfileEventPublisher {
  private readonly pending: ProfileTierChanged[] = [];
  private draining = false;

  constructor(
    private readonly brokerUrl: string,
    private readonly topic = 'profile.events',
    private readonly retryDelayMs = 200,
  ) {}

  publish(event: ProfileTierChanged): void {
    this.pending.push(event);
    void this.drain();
  }

  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    try {
      while (this.pending.length > 0) {
        const event = this.pending[0] as ProfileTierChanged;
        if (await this.send(event)) this.pending.shift();
        else await sleep(this.retryDelayMs);
      }
    } finally {
      this.draining = false;
    }
  }

  private async send(event: ProfileTierChanged): Promise<boolean> {
    try {
      const response = await fetch(`${this.brokerUrl}/topics/${this.topic}/messages`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Correlation-ID': event.correlationId },
        body: JSON.stringify(event),
        signal: AbortSignal.timeout(2000),
      });
      if (response.ok) return true;
      console.error(`[cdp] broker refused event ${event.eventId}: ${response.status}`);
    } catch (error) {
      console.error(`[cdp] broker unreachable for event ${event.eventId}: ${String(error)}`);
    }
    return false;
  }
}

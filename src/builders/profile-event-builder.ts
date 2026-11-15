import { randomUUID } from 'node:crypto';
import type { ProfileTier } from '../clients/cdp-types.js';
import type { ProfileTierChanged } from '../clients/notification-types.js';

const ORDER: ProfileTier[] = ['bronze', 'silver', 'gold'];

/**
 * A `profile.tier_changed` event as the customer data platform publishes it,
 * for notification tests that publish to the broker directly. Defaults: an
 * upgrade from bronze to silver with marketing consent.
 *
 *   const event = aProfileTierChanged(customerId, correlationId).to('gold').withConsent(null).build();
 *   const copy = aProfileTierChanged(customerId, correlationId).copyOf(event).build(); // new eventId, same source
 */
export class ProfileTierChangedBuilder {
  private event: ProfileTierChanged;

  constructor(customerId: string, correlationId: string) {
    this.event = {
      eventId: randomUUID(),
      type: 'profile.tier_changed',
      customerId,
      correlationId,
      occurredAt: new Date().toISOString(),
      sourceEventId: randomUUID(),
      data: {
        previousTier: 'bronze',
        newTier: 'silver',
        direction: 'upgrade',
        marketingConsent: true,
        transactionId: `txn_${randomUUID()}`,
      },
    };
  }

  from(tier: ProfileTier): this {
    this.event.data.previousTier = tier;
    return this.fixDirection();
  }

  to(tier: ProfileTier): this {
    this.event.data.newTier = tier;
    return this.fixDirection();
  }

  withConsent(consent: boolean | null): this {
    this.event.data.marketingConsent = consent;
    return this;
  }

  /** Same tier change (same sourceEventId), published again under a new eventId. */
  copyOf(other: ProfileTierChanged): this {
    this.event = { ...structuredClone(other), eventId: randomUUID() };
    return this;
  }

  build(): ProfileTierChanged {
    return structuredClone(this.event);
  }

  private fixDirection(): this {
    const { previousTier, newTier } = this.event.data;
    this.event.data.direction = ORDER.indexOf(newTier) > ORDER.indexOf(previousTier) ? 'upgrade' : 'downgrade';
    return this;
  }
}

export const aProfileTierChanged = (customerId: string, correlationId: string) =>
  new ProfileTierChangedBuilder(customerId, correlationId);

/** Notification service shapes, written from notification-openapi.yaml and cdp-asyncapi.yaml. */
import type { ProfileTier } from './cdp-types.js';

export type Outcome = 'sent' | 'duplicate' | 'suppressed-no-consent' | 'suppressed-consent-unknown';

export interface Decision {
  eventId: string;
  sourceEventId: string;
  outcome: Outcome;
  notificationId: string | null;
  decidedAt: string;
}

export interface Notification {
  notificationId: string;
  customerId: string;
  channel: 'email';
  template: 'tier-upgrade' | 'tier-downgrade';
  tier: ProfileTier;
  previousTier: ProfileTier;
  transactionId: string;
  sourceEventId: string;
  correlationId: string;
  createdAt: string;
}

/** The event the customer data platform publishes on profile.events. */
export interface ProfileTierChanged {
  eventId: string;
  type: 'profile.tier_changed';
  customerId: string;
  correlationId: string;
  occurredAt: string;
  sourceEventId: string;
  data: {
    previousTier: ProfileTier;
    newTier: ProfileTier;
    direction: 'upgrade' | 'downgrade';
    marketingConsent: boolean | null;
    transactionId: string;
  };
}

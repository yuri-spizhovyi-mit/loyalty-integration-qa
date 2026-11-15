import { randomUUID } from 'node:crypto';
import type { ProfileTier, ProfileTierChanged } from './events.js';

/**
 * Decides whether a profile event becomes a customer notification, and
 * records the result. Nothing is really sent.
 *
 * Rules:
 * - One notification per tier change. The key is `sourceEventId` (the
 *   engine's tier.changed event), so a redelivered message and a republished
 *   copy with a new eventId are both recognised as duplicates.
 * - Only with marketing consent. Consent that is unknown (null) is treated
 *   as no consent: never message someone who may not have agreed to it.
 * - Every event gets a decision, including duplicates and suppressed ones.
 *   That is what lets a test prove a negative: it waits for the decision,
 *   then checks that no notification exists, without sleeping.
 */

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

export class NotificationStore {
  private readonly notifications = new Map<string, Notification[]>();
  private readonly decisions = new Map<string, Decision[]>();
  private readonly bySourceEvent = new Map<string, string | null>();

  handle(event: ProfileTierChanged): Decision {
    const now = new Date().toISOString();
    const decide = (outcome: Outcome, notificationId: string | null): Decision => {
      const decision = { eventId: event.eventId, sourceEventId: event.sourceEventId, outcome, notificationId, decidedAt: now };
      this.listFor(this.decisions, event.customerId).push(decision);
      return decision;
    };

    if (this.bySourceEvent.has(event.sourceEventId)) {
      return decide('duplicate', this.bySourceEvent.get(event.sourceEventId) ?? null);
    }
    const consent = event.data.marketingConsent;
    if (consent !== true) {
      this.bySourceEvent.set(event.sourceEventId, null);
      return decide(consent === false ? 'suppressed-no-consent' : 'suppressed-consent-unknown', null);
    }

    const notification: Notification = {
      notificationId: `ntf_${randomUUID()}`,
      customerId: event.customerId,
      channel: 'email',
      template: event.data.direction === 'upgrade' ? 'tier-upgrade' : 'tier-downgrade',
      tier: event.data.newTier,
      previousTier: event.data.previousTier,
      transactionId: event.data.transactionId,
      sourceEventId: event.sourceEventId,
      correlationId: event.correlationId,
      createdAt: now,
    };
    this.listFor(this.notifications, event.customerId).push(notification);
    this.bySourceEvent.set(event.sourceEventId, notification.notificationId);
    return decide('sent', notification.notificationId);
  }

  notificationsFor(customerId: string): Notification[] {
    return structuredClone(this.notifications.get(customerId) ?? []);
  }

  decisionsFor(customerId: string): Decision[] {
    return structuredClone(this.decisions.get(customerId) ?? []);
  }

  private listFor<T>(map: Map<string, T[]>, customerId: string): T[] {
    let list = map.get(customerId);
    if (!list) {
      list = [];
      map.set(customerId, list);
    }
    return list;
  }
}

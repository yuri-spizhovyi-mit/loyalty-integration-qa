import { PactV4 } from '@pact-foundation/pact';
import { expect, test } from '@playwright/test';
import { eventHandler } from '../../../services/notification-service/src/app.js';
import { FaultInjector } from '../../../services/notification-service/src/faults.js';
import { NotificationStore } from '../../../services/notification-service/src/notifications.js';
import { EXAMPLE, type PactMessage, match, PACT_DIR, PARTICIPANTS, rawContent, resetPact } from '../../../src/pact/contracts.js';

/**
 * The notification service's contract with the customer data platform: the
 * `profile.tier_changed` event on `profile.events`. Consent is the part that
 * matters most, so there is one interaction per consent value. `null`
 * (consent not known yet) must stay `null`: the service suppresses the
 * message, which it would not do if the platform sent `false` or left the
 * field out.
 */

const { notifications: consumer, cdp: provider } = PARTICIPANTS;
const pact = new PactV4({ consumer, provider, dir: PACT_DIR, logLevel: 'warn' });

test.beforeAll(() => resetPact(consumer, provider));

const TIERS = ['bronze', 'silver', 'gold'] as const;

function tierChanged(marketingConsent: boolean | null) {
  return {
    eventId: match.uuid(EXAMPLE.eventId),
    type: 'profile.tier_changed',
    customerId: match.customerId(),
    correlationId: match.correlationId(),
    occurredAt: match.timestamp(),
    sourceEventId: match.uuid(EXAMPLE.sourceEventId),
    data: {
      previousTier: match.oneOf(TIERS, 'bronze'),
      newTier: match.oneOf(TIERS, 'silver'),
      direction: match.oneOf(['upgrade', 'downgrade'], 'upgrade'),
      // Exact values: true, false and null each lead to a different decision.
      marketingConsent,
      transactionId: match.transactionId(),
    },
  };
}

function consume(message: PactMessage) {
  const store = new NotificationStore();
  const handle = eventHandler(store, new FaultInjector());
  const result = handle(rawContent(message.contents.content), { messageId: 'pact', correlationId: EXAMPLE.correlationId });
  return { result, notifications: store.notificationsFor(EXAMPLE.customerId) };
}

test('an upgrade with consent: one notification, with the tier and the source event', async () => {
  await pact
    .addAsynchronousInteraction()
    .expectsToReceive('a profile.tier_changed event for a customer who consented', (builder) =>
      builder.withJSONContent(tierChanged(true)),
    )
    .executeTest(async (message) => {
      const { result, notifications } = consume(message);
      expect(result.status).toBe(200);
      expect(result.body).toMatchObject({ outcome: 'sent' });
      expect(notifications).toEqual([
        expect.objectContaining({
          template: 'tier-upgrade',
          tier: 'silver',
          previousTier: 'bronze',
          transactionId: EXAMPLE.transactionId,
          sourceEventId: EXAMPLE.sourceEventId,
        }),
      ]);
    });
});

test('a tier change without consent: no notification', async () => {
  await pact
    .addAsynchronousInteraction()
    .expectsToReceive('a profile.tier_changed event for a customer who did not consent', (builder) =>
      builder.withJSONContent(tierChanged(false)),
    )
    .executeTest(async (message) => {
      const { result, notifications } = consume(message);
      expect(result.status).toBe(200);
      expect(result.body).toMatchObject({ outcome: 'suppressed-no-consent' });
      expect(notifications).toEqual([]);
    });
});

test('a tier change before consent is known: no notification', async () => {
  await pact
    .addAsynchronousInteraction()
    .expectsToReceive('a profile.tier_changed event before the customer.created event arrived', (builder) =>
      builder.withJSONContent(tierChanged(null)),
    )
    .executeTest(async (message) => {
      const { result, notifications } = consume(message);
      expect(result.status).toBe(200);
      expect(result.body).toMatchObject({ outcome: 'suppressed-consent-unknown' });
      expect(notifications).toEqual([]);
    });
});

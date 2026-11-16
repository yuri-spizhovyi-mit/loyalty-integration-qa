import { PactV4 } from '@pact-foundation/pact';
import { expect, test } from '@playwright/test';
import { eventHandler } from '../../../services/customer-data-platform/src/app.js';
import { FaultInjector } from '../../../services/customer-data-platform/src/faults.js';
import { ProfileStore } from '../../../services/customer-data-platform/src/profiles.js';
import type { ProfileTierChanged } from '../../../services/customer-data-platform/src/publisher.js';
import { EXAMPLE, type PactMessage, loyaltyEnvelope, match, PACT_DIR, PARTICIPANTS, rawContent, resetPact } from '../../../src/pact/contracts.js';

/**
 * The customer data platform's contract with the loyalty engine: the five
 * loyalty events it consumes from `loyalty.events`, with only the fields and
 * formats its handler relies on. Each example is run through the platform's
 * real handler, and the test checks what it does with it: accepted (2xx) and
 * mapped into the profile.
 */

const { cdp: consumer, engine: provider } = PARTICIPANTS;
const pact = new PactV4({ consumer, provider, dir: PACT_DIR, logLevel: 'warn' });

test.beforeAll(() => resetPact(consumer, provider));

/** Runs a message through the CDP's own handler, as the Kafka consumer or the push endpoint would. */
function consume(message: PactMessage) {
  const profiles = new ProfileStore();
  const published: ProfileTierChanged[] = [];
  const handle = eventHandler(profiles, new FaultInjector(), { publish: (event) => published.push(event) });
  const result = handle(rawContent(message.contents.content), { messageId: 'pact', correlationId: EXAMPLE.correlationId });
  return { result, profile: profiles.get(EXAMPLE.customerId), published };
}

const transactionData = (entryType: string, points: number, balanceAfter: number, refundOf: string | null) => ({
  transactionId: match.transactionId(),
  entryType,
  points: match.integer(points),
  balanceAfter: match.integer(balanceAfter),
  reference: match.nullOr(entryType === 'EARN' ? 'order-1001' : null),
  refundOf: refundOf === null ? null : match.transactionId(refundOf),
});

test('customer.created: a new member becomes a Bronze profile with their consent', async () => {
  await pact
    .addAsynchronousInteraction()
    .expectsToReceive('a customer.created event', (builder) =>
      builder.withJSONContent(
        loyaltyEnvelope('customer.created', 1, { tier: 'Bronze', marketingConsent: match.boolean(true) }),
      ),
    )
    .executeTest(async (message) => {
      const { result, profile } = consume(message);
      expect(result.status).toBe(200);
      expect(profile?.attributes).toMatchObject({ loyalty_tier: 'bronze', marketing_consent: true, points_balance: 0 });
    });
});

test('points.earned: the balance and last transaction are mapped', async () => {
  await pact
    .addAsynchronousInteraction()
    .expectsToReceive('a points.earned event', (builder) =>
      builder.withJSONContent(loyaltyEnvelope('points.earned', 2, transactionData('EARN', 250, 250, null))),
    )
    .executeTest(async (message) => {
      const { result, profile } = consume(message);
      expect(result.status).toBe(200);
      expect(profile?.attributes).toMatchObject({
        points_balance: 250,
        last_transaction_id: EXAMPLE.transactionId,
        last_transaction_type: 'earn',
      });
    });
});

test('points.redeemed: the new balance is mapped', async () => {
  await pact
    .addAsynchronousInteraction()
    .expectsToReceive('a points.redeemed event', (builder) =>
      builder.withJSONContent(loyaltyEnvelope('points.redeemed', 3, transactionData('REDEEM', -100, 150, null))),
    )
    .executeTest(async (message) => {
      const { result, profile } = consume(message);
      expect(result.status).toBe(200);
      expect(profile?.attributes).toMatchObject({ points_balance: 150, last_transaction_type: 'redeem' });
    });
});

test('points.refunded: the new balance is mapped, and the refund points at the earn', async () => {
  await pact
    .addAsynchronousInteraction()
    .expectsToReceive('a points.refunded event', (builder) =>
      builder.withJSONContent(
        loyaltyEnvelope('points.refunded', 3, transactionData('REFUND', -250, 0, EXAMPLE.earnTransactionId)),
      ),
    )
    .executeTest(async (message) => {
      const { result, profile } = consume(message);
      expect(result.status).toBe(200);
      expect(profile?.attributes).toMatchObject({ points_balance: 0, last_transaction_type: 'refund' });
    });
});

test('tier.changed: the profile tier is mapped and a profile.tier_changed is published', async () => {
  const tiers = ['Bronze', 'Silver', 'Gold'] as const;
  await pact
    .addAsynchronousInteraction()
    .expectsToReceive('a tier.changed event', (builder) =>
      builder.withJSONContent(
        loyaltyEnvelope('tier.changed', 3, {
          previousTier: match.oneOf(tiers, 'Bronze'),
          newTier: match.oneOf(tiers, 'Silver'),
          direction: match.oneOf(['upgrade', 'downgrade'], 'upgrade'),
          qualifyingPoints: match.integer(1000),
          transactionId: match.transactionId(),
        }),
      ),
    )
    .executeTest(async (message) => {
      const { result, profile, published } = consume(message);
      expect(result.status).toBe(200);
      expect(profile?.attributes.loyalty_tier).toBe('silver');
      expect(published.map((event) => [event.type, event.sourceEventId, event.data.newTier])).toEqual([
        ['profile.tier_changed', EXAMPLE.eventId, 'silver'],
      ]);
    });
});


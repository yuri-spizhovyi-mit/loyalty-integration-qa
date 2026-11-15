import { anEarn, aRedeem, aRefundOf, uniqueIdempotencyKey } from '../../src/builders/transaction-builder.js';
import type { LoyaltyClient } from '../../src/clients/loyalty-client.js';
import { aCustomer } from '../../src/builders/customer-builder.js';
import type {
  CustomerCreatedData,
  LedgerResponse,
  LoyaltyEvent,
  OutboxResponse,
  TierChangedData,
  TransactionEventData,
} from '../../src/clients/loyalty-types.js';
import { expect, test } from '../../src/fixtures.js';
import { readJson } from '../../src/helpers/json.js';
import { getContract } from '../../src/helpers/schema-validator.js';

/**
 * Events as the engine publishes them (read from its outbox), checked
 * against asyncapi.yaml. Step 4 checks the same events after the queue.
 */

async function outbox(loyalty: LoyaltyClient, customerId: string): Promise<LoyaltyEvent[]> {
  return (await readJson<OutboxResponse>(await loyalty.getOutbox(customerId))).events;
}

/**
 * Customer created, then earn 1200 (Bronze -> Silver), redeem 100, refund 300 of the earn
 * (qualifying 900: Silver -> Bronze). Publishes every event type.
 */
async function fullLifecycle(loyalty: LoyaltyClient, customerId: string) {
  const earn = await loyalty.earn(customerId, anEarn().points(1200).build());
  const earnTxId = ((await earn.json()) as { transaction: { transactionId: string } }).transaction.transactionId;
  await expect(await loyalty.redeem(customerId, aRedeem().points(100).build())).toHaveStatus(201);
  await expect(await loyalty.refund(customerId, aRefundOf(earnTxId).points(300).build())).toHaveStatus(201);
  return outbox(loyalty, customerId);
}

test.describe('Events match asyncapi.yaml', () => {
  test('every published event matches its AsyncAPI message', async ({ loyalty, customer }) => {
    const events = await fullLifecycle(loyalty, customer.customerId);

    expect(events.length).toBeGreaterThan(0);
    for (const event of events) expect(event as unknown).toMatchEventSchema();
  });

  test('the lifecycle publishes every message type in asyncapi.yaml, in order', async ({ loyalty, customer }) => {
    const events = await fullLifecycle(loyalty, customer.customerId);

    expect(events.map((event) => event.type)).toEqual([
      'customer.created',
      'points.earned',
      'tier.changed',
      'points.redeemed',
      'points.refunded',
      'tier.changed',
    ]);
    expect(new Set(events.map((event) => event.type))).toEqual(new Set(getContract().eventTypes()));
  });

  test('sequence numbers run 1..n per customer and eventIds are unique', async ({ loyalty, customer }) => {
    const events = await fullLifecycle(loyalty, customer.customerId);

    expect(events.map((event) => event.sequence)).toEqual(events.map((_, index) => index + 1));
    expect(new Set(events.map((event) => event.eventId)).size).toBe(events.length);
  });

  test('every event carries the customerId and the X-Correlation-ID of its request', async ({
    loyalty,
    customer,
    correlationId,
  }) => {
    const events = await fullLifecycle(loyalty, customer.customerId);

    for (const event of events) {
      expect(event).toMatchObject({ customerId: customer.customerId, correlationId });
    }
  });

  test('event data matches the ledger entry it came from (field mapping)', async ({ loyalty, customer }) => {
    const events = await fullLifecycle(loyalty, customer.customerId);
    const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(customer.customerId));

    const transactionEvents = events.filter((event) => event.type.startsWith('points.')) as unknown as LoyaltyEvent<TransactionEventData>[];
    expect(transactionEvents).toHaveLength(ledger.entries.length);
    for (const event of transactionEvents) {
      const entry = ledger.entries.find((candidate) => candidate.transactionId === event.data.transactionId);
      expect(entry, `ledger entry for ${event.type} ${event.data.transactionId}`).toBeDefined();
      expect(event.data).toEqual({
        transactionId: entry!.transactionId,
        entryType: entry!.type,
        points: entry!.points,
        balanceAfter: entry!.balanceAfter,
        reference: entry!.reference,
        refundOf: entry!.refundOf,
      });
    }
  });

  test('tier.changed follows its transaction event and really changes the tier', async ({ loyalty, customer }) => {
    const events = await fullLifecycle(loyalty, customer.customerId);

    events.forEach((event, index) => {
      if (event.type !== 'tier.changed') return;
      const data = event.data as unknown as TierChangedData;
      const previous = events[index - 1] as LoyaltyEvent<TransactionEventData> | undefined;
      expect(previous?.data.transactionId, 'tier.changed must follow the event that caused it').toBe(data.transactionId);
      expect(data.newTier).not.toBe(data.previousTier);
    });
    const changes = events
      .filter((event) => event.type === 'tier.changed')
      .map((event) => event.data as unknown as TierChangedData);
    expect(changes.map(({ previousTier, newTier, direction }) => [previousTier, newTier, direction])).toEqual([
      ['Bronze', 'Silver', 'upgrade'],
      ['Silver', 'Bronze', 'downgrade'],
    ]);
  });

  test('customer.created is the first event and carries the consent flag', async ({ loyalty, createCustomer }) => {
    const { customerId } = await createCustomer(aCustomer().withMarketingConsent(true).build());

    const events = await outbox(loyalty, customerId);
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ type: 'customer.created', sequence: 1, customerId });
    expect(events[0]?.data as unknown as CustomerCreatedData).toEqual({ tier: 'Bronze', marketingConsent: true });
  });

  test('a rejected customer creation publishes nothing', async ({ loyalty, customer }) => {
    await expect(await loyalty.createCustomer({ customerId: customer.customerId })).toHaveStatus(409);

    expect((await outbox(loyalty, customer.customerId)).map((event) => event.type)).toEqual(['customer.created']);
  });

  test('rejected transactions publish nothing', async ({ loyalty, customer }) => {
    await loyalty.earn(customer.customerId, { points: 0 });
    await loyalty.redeem(customer.customerId, aRedeem().points(10).build());
    await loyalty.refund(customer.customerId, aRefundOf('txn_unknown').build());

    expect((await outbox(loyalty, customer.customerId)).map((event) => event.type)).toEqual(['customer.created']);
  });

  test('an idempotent replay publishes nothing extra', async ({ loyalty, customer }) => {
    const key = uniqueIdempotencyKey();
    const request = anEarn().points(50).build();

    await loyalty.earn(customer.customerId, request, { idempotencyKey: key });
    await loyalty.earn(customer.customerId, request, { idempotencyKey: key });

    expect((await outbox(loyalty, customer.customerId)).map((event) => event.type)).toEqual(['customer.created', 'points.earned']);
  });
});

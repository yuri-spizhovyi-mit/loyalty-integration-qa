import { expect, test } from '../../src/fixtures.js';
import { deliverInOrder } from '../../src/helpers/delivery.js';

/**
 * The broker does not guarantee order (a retried message arrives after later
 * ones). The consumer uses `sequence`: each group of profile fields keeps the
 * sequence of the event that last wrote it, and older events never overwrite
 * newer ones. Each test delivers events one by one in a chosen order.
 */

test.describe('Out-of-order events', () => {
  test('a stream delivered in reverse ends in the same profile as in order', async ({ cdp, queues, eventStream }) => {
    const created = eventStream.customerCreated(true);
    const earnA = eventStream.earned(500);
    const earnB = eventStream.earned(700); // balance 1200
    const upgrade = eventStream.tierChanged('Bronze', 'Silver', 1200, earnB);
    const redeem = eventStream.redeemed(200); // balance 1000
    const stream = [created, earnA, earnB, upgrade, redeem];

    await deliverInOrder(queues, [...stream].reverse());

    const profile = await cdp.profileOrNull(eventStream.customerId);
    expect(profile?.attributes).toEqual({
      loyalty_tier: 'silver',
      points_balance: 1000,
      marketing_consent: true,
      member_since: created.occurredAt,
      last_transaction_id: redeem.data.transactionId,
      last_transaction_type: 'redeem',
      last_activity_at: redeem.occurredAt,
    });
    expect(profile?.sync).toMatchObject({
      lastSequence: 5,
      appliedEvents: 5,
      outOfOrderEvents: 4,
      missingSequences: [],
    });
  });

  test('a late tier upgrade does not overwrite a newer downgrade', async ({ cdp, queues, eventStream }) => {
    const created = eventStream.customerCreated();
    const earn = eventStream.earned(1200);
    const upgrade = eventStream.tierChanged('Bronze', 'Silver', 1200, earn);
    const refund = eventStream.refunded(300, earn);
    const downgrade = eventStream.tierChanged('Silver', 'Bronze', 900, refund);

    await deliverInOrder(queues, [created, earn, refund, downgrade, upgrade]);

    const profile = await cdp.profileOrNull(eventStream.customerId);
    expect(profile?.attributes.loyalty_tier, 'the downgrade (sequence 5) is newer than the upgrade (sequence 3)').toBe(
      'bronze',
    );
    expect(profile?.attributes.points_balance).toBe(900);
  });

  test('a late older transaction does not overwrite the balance', async ({ cdp, queues, eventStream }) => {
    const created = eventStream.customerCreated();
    const first = eventStream.earned(100);
    const second = eventStream.earned(200);

    await deliverInOrder(queues, [created, second, first]);

    const profile = await cdp.profileOrNull(eventStream.customerId);
    expect(profile?.attributes).toMatchObject({ points_balance: 300, last_transaction_id: second.data.transactionId });
    expect(profile?.sync.outOfOrderEvents).toBe(1);
  });

  test('a transaction that arrives before customer.created keeps its balance', async ({ cdp, queues, eventStream }) => {
    const created = eventStream.customerCreated(true);
    const earn = eventStream.earned(100);

    await deliverInOrder(queues, [earn]);
    const early = await cdp.profileOrNull(eventStream.customerId);
    expect(early?.attributes).toMatchObject({ points_balance: 100, loyalty_tier: null, marketing_consent: null });
    expect(early?.sync.missingSequences).toEqual([1]);

    await deliverInOrder(queues, [created]);
    const late = await cdp.profileOrNull(eventStream.customerId);
    expect(late?.attributes, 'customer.created must not reset the balance to 0').toMatchObject({
      points_balance: 100,
      loyalty_tier: 'bronze',
      marketing_consent: true,
    });
    expect(late?.sync.missingSequences).toEqual([]);
  });

  test('a gap in the sequence is reported until the missing event arrives', async ({ cdp, queues, eventStream }) => {
    const created = eventStream.customerCreated();
    const first = eventStream.earned(100);
    const second = eventStream.earned(100);
    const third = eventStream.earned(100);

    await deliverInOrder(queues, [created, third]);
    expect((await cdp.profileOrNull(eventStream.customerId))?.sync).toMatchObject({
      lastSequence: 4,
      missingSequences: [2, 3],
    });

    await deliverInOrder(queues, [second]);
    expect((await cdp.profileOrNull(eventStream.customerId))?.sync.missingSequences).toEqual([2]);

    await deliverInOrder(queues, [first]);
    const profile = await cdp.profileOrNull(eventStream.customerId);
    expect(profile?.sync.missingSequences).toEqual([]);
    expect(profile?.attributes.points_balance).toBe(300);
  });
});

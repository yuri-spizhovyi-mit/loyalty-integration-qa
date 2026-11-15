import { randomUUID } from 'node:crypto';
import { aCustomer } from '../../src/builders/customer-builder.js';
import { anEarn, aRedeem, aRefundOf, uniqueIdempotencyKey } from '../../src/builders/transaction-builder.js';
import type { OutboxResponse } from '../../src/clients/loyalty-types.js';
import { PROFILE_TOPIC } from '../../src/config.js';
import { expect, test } from '../../src/fixtures.js';
import { readJson } from '../../src/helpers/json.js';
import { NOTIFICATION_SLA_MS, settleNotifications } from '../../src/helpers/notification-wait.js';
import { inParallel } from '../../src/helpers/parallel.js';
import { POLL } from '../../src/helpers/polling.js';

/**
 * Notification triggers, end to end: REST call on the engine -> tier.changed
 * -> customer data platform -> profile.tier_changed -> notification service.
 *
 * "No notification" is never checked by waiting a fixed time. Every tier
 * change gets a decision, so the tests wait for the decision and then look.
 * Where nothing should happen at all, a later write that does change the
 * tier acts as a marker: once its decision is in, anything the earlier
 * writes might have triggered would have arrived too.
 *
 * The broker does not keep order, and the customer data platform ignores a
 * tier change that arrives after a newer one (see tests/events). So tests
 * with several tier changes wait for each one before making the next.
 */

const withConsent = () => aCustomer().withMarketingConsent(true).build();

test.describe('Tier notifications', () => {
  test(`an upgrade with consent records exactly one notification within ${NOTIFICATION_SLA_MS} ms`, async ({
    loyalty,
    notifications,
    createCustomer,
    earnPoints,
    correlationId,
  }) => {
    const { customerId } = await createCustomer(withConsent());
    const earn = await earnPoints(customerId, 1000);

    const result = await settleNotifications(loyalty, notifications, customerId);
    expect(result.decisions.map((d) => d.outcome)).toEqual(['sent']);
    expect(result.notifications).toHaveLength(1);
    expect(result.notifications[0]).toMatchObject({
      customerId,
      channel: 'email',
      template: 'tier-upgrade',
      previousTier: 'bronze',
      tier: 'silver',
      transactionId: earn.transaction.transactionId,
      sourceEventId: result.tierChangeIds[0],
      correlationId,
    });
    expect(result.decisions[0]!.notificationId).toBe(result.notifications[0]!.notificationId);
  });

  test('an upgrade without consent records no notification', async ({ loyalty, notifications, createCustomer, earnPoints }) => {
    const { customerId } = await createCustomer(aCustomer().withMarketingConsent(false).build());
    await earnPoints(customerId, 1000);

    const result = await settleNotifications(loyalty, notifications, customerId);
    expect(result.decisions.map((d) => d.outcome)).toEqual(['suppressed-no-consent']);
    expect(result.notifications).toEqual([]);
  });

  test('Bronze to Gold in one earn is one notification, not two', async ({ loyalty, notifications, createCustomer, earnPoints }) => {
    const { customerId } = await createCustomer(withConsent());
    await earnPoints(customerId, 5000);

    const result = await settleNotifications(loyalty, notifications, customerId);
    expect(result.notifications.map((n) => `${n.previousTier}->${n.tier}`)).toEqual(['bronze->gold']);
  });

  test('each tier change gets its own notification: up, up, then down after a refund', async ({
    loyalty,
    notifications,
    createCustomer,
    earnPoints,
  }) => {
    const { customerId } = await createCustomer(withConsent());
    await earnPoints(customerId, 1000);
    await settleNotifications(loyalty, notifications, customerId);
    const gold = await earnPoints(customerId, 4000);
    await settleNotifications(loyalty, notifications, customerId);
    await expect(await loyalty.refund(customerId, aRefundOf(gold.transaction.transactionId).build())).toHaveStatus(201);

    const result = await settleNotifications(loyalty, notifications, customerId);
    expect(result.notifications.map((n) => `${n.template} ${n.tier}`)).toEqual([
      'tier-upgrade silver',
      'tier-upgrade gold',
      'tier-downgrade silver',
    ]);
    expect(result.notifications.map((n) => n.sourceEventId)).toEqual(result.tierChangeIds);
  });

  test('redeeming points does not lower the tier, so it sends nothing', async ({
    loyalty,
    notifications,
    createCustomer,
    earnPoints,
  }) => {
    const { customerId } = await createCustomer(withConsent());
    await earnPoints(customerId, 1200);
    await settleNotifications(loyalty, notifications, customerId);
    const redeem = await loyalty.redeem(customerId, aRedeem().points(1200).build());
    await expect(redeem).toHaveStatus(201);
    await earnPoints(customerId, 4000); // marker: Silver -> Gold

    const result = await settleNotifications(loyalty, notifications, customerId);
    expect(result.notifications.map((n) => n.tier)).toEqual(['silver', 'gold']);
  });

  test('failed transactions send nothing', async ({ loyalty, notifications, createCustomer, earnPoints }) => {
    const { customerId } = await createCustomer(withConsent());
    const earn = await earnPoints(customerId, 990);
    const failures = [
      await loyalty.redeem(customerId, aRedeem().points(5000).build()), // more than the balance
      await loyalty.earn(customerId, { points: 2_000_000 }), // above the per-transaction maximum
      await loyalty.earn(customerId, { points: 10, bonus: true }), // unknown field
      await loyalty.refund(customerId, aRefundOf(earn.transaction.transactionId).points(991).build()), // more than earned
      await loyalty.refund(customerId, aRefundOf(`txn_${randomUUID()}`).build()), // unknown transaction
    ];
    for (const failure of failures) expect(failure.status(), await failure.text()).toBeGreaterThanOrEqual(400);

    const { events } = await readJson<OutboxResponse>(await loyalty.getOutbox(customerId));
    expect(events.map((e) => e.type), 'failed requests publish no events').toEqual(['customer.created', 'points.earned']);

    const marker = await earnPoints(customerId, 10); // 1000 points: the only tier change
    const result = await settleNotifications(loyalty, notifications, customerId);
    expect(result.notifications).toHaveLength(1);
    expect(result.notifications[0]!.transactionId).toBe(marker.transaction.transactionId);
  });

  test('an idempotent replay of the upgrading earn sends one notification', async ({
    loyalty,
    notifications,
    createCustomer,
  }) => {
    const { customerId } = await createCustomer(withConsent());
    const key = uniqueIdempotencyKey();
    const body = anEarn().points(1000).build();
    for (let attempt = 0; attempt < 3; attempt += 1) {
      await expect(await loyalty.earn(customerId, body, { idempotencyKey: key })).toHaveStatus(201);
    }

    const result = await settleNotifications(loyalty, notifications, customerId);
    expect(result.tierChangeIds).toHaveLength(1);
    expect(result.notifications).toHaveLength(1);
  });

  test('parallel earns that cross the threshold together send one upgrade', async ({
    loyalty,
    notifications,
    createCustomer,
  }) => {
    const { customerId } = await createCustomer(withConsent());
    const responses = await inParallel(loyalty, 10, () => loyalty.earn(customerId, anEarn().points(100).build()));
    for (const response of responses) await expect(response).toHaveStatus(201);

    const result = await settleNotifications(loyalty, notifications, customerId);
    expect(result.notifications.map((n) => n.tier)).toEqual(['silver']);
  });

  test("customers' notifications do not mix", async ({ loyalty, notifications, createCustomer, earnPoints }) => {
    const [first, second] = await Promise.all([createCustomer(withConsent()), createCustomer(withConsent())]);
    await Promise.all([earnPoints(first.customerId, 1000), earnPoints(second.customerId, 5000)]);

    const [one, two] = await Promise.all([
      settleNotifications(loyalty, notifications, first.customerId),
      settleNotifications(loyalty, notifications, second.customerId),
    ]);
    expect(one.notifications.map((n) => `${n.customerId} ${n.tier}`)).toEqual([`${first.customerId} silver`]);
    expect(two.notifications.map((n) => `${n.customerId} ${n.tier}`)).toEqual([`${second.customerId} gold`]);
  });
});

test.describe('Profile events published by the customer data platform', () => {
  test('one profile.tier_changed per engine tier change, each matching cdp-asyncapi.yaml', async ({
    loyalty,
    notifications,
    queues,
    createCustomer,
    earnPoints,
    correlationId,
  }) => {
    const { customerId } = await createCustomer(aCustomer().withMarketingConsent(false).build());
    await earnPoints(customerId, 1000);
    await settleNotifications(loyalty, notifications, customerId);
    const gold = await earnPoints(customerId, 4000);
    await settleNotifications(loyalty, notifications, customerId);
    await expect(await loyalty.refund(customerId, aRefundOf(gold.transaction.transactionId).build())).toHaveStatus(201);
    const { tierChangeIds } = await settleNotifications(loyalty, notifications, customerId);

    await expect
      .poll(async () => (await queues.messagesOn(PROFILE_TOPIC, customerId)).length, POLL)
      .toBe(tierChangeIds.length);
    const published = (await queues.messagesOn(PROFILE_TOPIC, customerId)).map((m) => JSON.parse(m.payload));
    for (const event of published) expect(event as unknown).toMatchEventSchema();
    expect(published.map((e) => e.sourceEventId)).toEqual(tierChangeIds);
    expect(published.map((e) => `${e.data.previousTier}->${e.data.newTier} consent=${e.data.marketingConsent}`)).toEqual([
      'bronze->silver consent=false',
      'silver->gold consent=false',
      'gold->silver consent=false',
    ]);
    expect(new Set(published.map((e) => e.correlationId))).toEqual(new Set([correlationId]));
  });
});

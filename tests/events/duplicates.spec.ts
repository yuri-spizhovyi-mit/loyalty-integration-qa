import { expect, test } from '../../src/fixtures.js';
import { deliverInOrder, waitForDelivery } from '../../src/helpers/delivery.js';
import { readJson } from '../../src/helpers/json.js';
import { POLL } from '../../src/helpers/polling.js';

/**
 * The broker delivers at least once, so the same event can arrive twice.
 * The consumer must acknowledge the copy (or the broker keeps retrying) and
 * must not apply it again.
 *
 * Note: points_balance is copied from `balanceAfter`, so applying a duplicate
 * would not change the balance by itself. The tests therefore also check the
 * sync counters, which do show a second apply.
 */

test.describe('Duplicate events', () => {
  test('the same event published twice is applied once and both copies are acknowledged', async ({
    cdp,
    queues,
    eventStream,
  }) => {
    const created = eventStream.customerCreated();
    const earned = eventStream.earned(100);
    await deliverInOrder(queues, [created, earned]);

    const copy = await queues.publish(earned);
    await waitForDelivery(queues, copy, 'delivered');

    const profile = await cdp.profileOrNull(eventStream.customerId);
    expect(profile?.sync).toMatchObject({ appliedEvents: 2, duplicateEvents: 1 });
    expect(profile?.attributes.points_balance).toBe(100);
  });

  test('a late duplicate does not roll the profile back', async ({ cdp, queues, eventStream }) => {
    const created = eventStream.customerCreated();
    const first = eventStream.earned(100); // balanceAfter 100
    const second = eventStream.earned(50); // balanceAfter 150
    await deliverInOrder(queues, [created, first, second, first]);

    const profile = await cdp.profileOrNull(eventStream.customerId);
    expect(profile?.attributes).toMatchObject({
      points_balance: 150,
      last_transaction_id: second.data.transactionId,
    });
    expect(profile?.sync).toMatchObject({ appliedEvents: 3, duplicateEvents: 1 });
  });

  test('a lost acknowledgement causes a redelivery that is recognised as a duplicate', async ({
    cdp,
    queues,
    eventStream,
  }) => {
    const created = eventStream.customerCreated();
    await deliverInOrder(queues, [created]);
    // The consumer applies the next event but its 200 never reaches the broker.
    await expect(await cdp.setFault(eventStream.customerId, 'fail-after-apply', 1)).toHaveStatus(204);

    const earned = eventStream.earned(100);
    const delivery = await waitForDelivery(queues, await queues.publish(earned), 'delivered');

    expect(delivery.history.map((attempt) => attempt.outcome)).toEqual(['retry', 'ack']);
    const profile = await cdp.profileOrNull(eventStream.customerId);
    expect(profile?.sync).toMatchObject({ appliedEvents: 2, duplicateEvents: 1 });
  });

  test('the consumer answers "duplicate" to a copy sent straight to it', async ({ cdp, eventStream }) => {
    const created = JSON.stringify(eventStream.customerCreated());

    const first = await cdp.sendEvent(created);
    const second = await cdp.sendEvent(created);

    await expect(first).toHaveStatus(200);
    await expect(second).toHaveStatus(200);
    expect(await readJson<{ result: string }>(first)).toMatchObject({ result: 'applied' });
    expect(await readJson<{ result: string }>(second)).toMatchObject({ result: 'duplicate' });
  });

  test('duplicates across many redeliveries still count once', async ({ cdp, queues, eventStream }) => {
    const created = eventStream.customerCreated();
    const messageIds = await Promise.all(Array.from({ length: 5 }, () => queues.publish(created)));
    for (const messageId of messageIds) await waitForDelivery(queues, messageId, 'delivered');

    await expect
      .poll(async () => (await cdp.profileOrNull(eventStream.customerId))?.sync, POLL)
      .toMatchObject({ appliedEvents: 1, duplicateEvents: 4 });
  });
});

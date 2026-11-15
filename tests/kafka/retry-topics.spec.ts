import { CDP_QUEUE } from '../../src/config.js';
import { expect, test } from '../../src/fixtures.js';
import { deliverInOrder, waitForDelivery } from '../../src/helpers/delivery.js';
import { customersOnPartitions } from '../../src/helpers/kafka-placement.js';

/**
 * Kafka with non-blocking retries (KAFKA_RETRY_MODE=topics): a failing
 * record is parked on `<group>.retry-1`, `retry-2`, `retry-3` (100, 200,
 * 400 ms), and its partition moves on. No head-of-line blocking, but a
 * later event of the same customer can now overtake a retried one, so the
 * consumer has to cope with reordering. Compare blocking-retries.spec.ts.
 */

test.describe('Kafka retry topics: the partition keeps moving', () => {
  test('a record being retried does not hold back later records on its partition', async ({
    queues,
    cdp,
    correlationId,
  }) => {
    const { slow, sameLane } = await customersOnPartitions(queues, correlationId, [
      { role: 'slow' },
      { role: 'sameLane', samePartitionAs: 'slow' },
    ]);
    try {
      await cdp.setFault(slow!.stream.customerId, 'fail-before-apply', 3);
      const slowId = await queues.publish(slow!.stream.earned(10));
      const sameLaneId = await queues.publish(sameLane!.stream.earned(10));

      const [slowDelivery, sameLaneDelivery] = await Promise.all(
        [slowId, sameLaneId].map((id) => waitForDelivery(queues, id, 'delivered')),
      );
      expect(slowDelivery!.attempts, 'still delivered on the 4th attempt').toBe(4);
      expect(sameLaneDelivery!.attempts).toBe(1);
      expect(
        Date.parse(sameLaneDelivery!.deliveredAt!),
        'the record behind it on the same partition went first',
      ).toBeLessThan(Date.parse(slowDelivery!.deliveredAt!));
    } finally {
      await cdp.clearFault(slow!.stream.customerId);
    }
  });

  test('the partition commits past a failing record once it is parked on a retry topic', async ({
    queues,
    cdp,
    eventStream,
  }) => {
    await deliverInOrder(queues, [eventStream.customerCreated()]);
    await cdp.setFault(eventStream.customerId, 'fail-before-apply', 3);
    const placement = await queues.publishWithPlacement(eventStream.earned(10));
    // One sample = the committed offset and the delivery status read together.
    // Wanted: committed past the record while it is still on its way through
    // the retry topics (about 700 ms in total).
    const sample = async () => {
      const [groups, delivery] = await Promise.all([queues.consumerGroups(), queues.delivery(placement.messageId)]);
      const group = groups.find((g) => g.consumerGroup === CDP_QUEUE)!;
      const committed = BigInt(group.committedOffsets?.[String(placement.partition)] ?? '0');
      if (delivery.status !== 'pending') return `finished (${delivery.status}) before the commit was seen`;
      return committed > BigInt(placement.offset!) ? 'committed while still pending' : 'waiting';
    };
    await expect
      .poll(sample, { intervals: [10, 25, 50], timeout: 5_000, message: 'commit while the record is parked' })
      .toBe('committed while still pending');

    const delivery = await waitForDelivery(queues, placement.messageId, 'delivered');
    expect(delivery.history.map((h) => h.outcome)).toEqual(['retry', 'retry', 'retry', 'ack']);
    // The waits between attempts still follow the backoff: at least 100, 200, 400 ms.
    const times = delivery.history.map((h) => Date.parse(h.at));
    const gaps = times.slice(1).map((t, i) => t - times[i]!);
    expect(gaps[0]).toBeGreaterThanOrEqual(100);
    expect(gaps[1]).toBeGreaterThanOrEqual(200);
    expect(gaps[2]).toBeGreaterThanOrEqual(400);
  });
});

test.describe('Kafka retry topics: the price is reordering', () => {
  test("a customer's later event overtakes a retried one, and the profile is still right", async ({
    queues,
    cdp,
    eventStream,
  }) => {
    await deliverInOrder(queues, [eventStream.customerCreated()]);
    await cdp.setFault(eventStream.customerId, 'fail-before-apply', 1);
    const first = eventStream.earned(10); // sequence 2, balanceAfter 10: fails once, then retried
    const second = eventStream.earned(5); // sequence 3, balanceAfter 15
    const [firstId, secondId] = await queues.publishAll([first, second]);

    const [firstDelivery, secondDelivery] = await Promise.all(
      [firstId!, secondId!].map((id) => waitForDelivery(queues, id, 'delivered')),
    );
    expect(Date.parse(secondDelivery!.deliveredAt!), 'sequence 3 was applied before sequence 2').toBeLessThan(
      Date.parse(firstDelivery!.deliveredAt!),
    );

    // Sequence versioning: the late sequence 2 does not roll the balance back to 10.
    const profile = await cdp.profileOrNull(eventStream.customerId);
    expect(profile?.attributes.points_balance).toBe(15);
    expect(profile?.attributes.last_transaction_id).toBe(second.data.transactionId);
    expect(profile?.sync).toMatchObject({ lastSequence: 3, appliedEvents: 3, outOfOrderEvents: 1, missingSequences: [] });
  });
});

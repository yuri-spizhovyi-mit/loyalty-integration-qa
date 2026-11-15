import { CDP_QUEUE } from '../../src/config.js';
import { expect, test } from '../../src/fixtures.js';
import { deliverInOrder, waitForDelivery } from '../../src/helpers/delivery.js';
import { customersOnPartitions, PARTITIONS } from '../../src/helpers/kafka-placement.js';
import { POLL } from '../../src/helpers/polling.js';

/**
 * Kafka with blocking retries (KAFKA_RETRY_MODE=blocking, the default): a
 * failing record is retried in place, so its partition waits. Order per
 * customer is kept; the price is head-of-line blocking for the other
 * customers on the partition. Compare retry-topics.spec.ts.
 */

test.describe('Kafka: retries block their own partition only', () => {
  test('a record being retried holds back later records on its partition, not on others', async ({
    queues,
    cdp,
    correlationId,
  }) => {
    const { slow, sameLane, otherLane } = await customersOnPartitions(queues, correlationId, [
      { role: 'slow' },
      { role: 'sameLane', samePartitionAs: 'slow' },
      { role: 'otherLane' },
    ]);
    try {
      // Three failures: delivered on the 4th attempt after 100 + 200 + 400 ms of backoff.
      await cdp.setFault(slow!.stream.customerId, 'fail-before-apply', 3);
      const slowId = await queues.publish(slow!.stream.earned(10));
      const sameLaneId = await queues.publish(sameLane!.stream.earned(10));
      const otherLaneId = await queues.publish(otherLane!.stream.earned(10));

      const [slowDelivery, sameLaneDelivery, otherLaneDelivery] = await Promise.all(
        [slowId, sameLaneId, otherLaneId].map((id) => waitForDelivery(queues, id, 'delivered')),
      );
      expect(slowDelivery!.attempts).toBe(4);
      const at = (d: typeof slowDelivery) => Date.parse(d!.deliveredAt!);
      expect(at(otherLaneDelivery), 'another partition is not held back').toBeLessThan(at(slowDelivery));
      expect(at(sameLaneDelivery), 'the same partition waits for the retried record').toBeGreaterThanOrEqual(at(slowDelivery));
      expect(sameLaneDelivery!.attempts).toBe(1);
    } finally {
      await cdp.clearFault(slow!.stream.customerId);
    }
  });
});

test.describe('Kafka: offsets', () => {
  test('the offset is committed only after the record is handled', async ({ queues, cdp, eventStream }) => {
    await deliverInOrder(queues, [eventStream.customerCreated()]);
    await cdp.setFault(eventStream.customerId, 'fail-before-apply', 3);
    const placement = await queues.publishWithPlacement(eventStream.earned(10));
    const partition = String(placement.partition);
    const committed = async () => {
      const group = (await queues.consumerGroups()).find((g) => g.consumerGroup === CDP_QUEUE)!;
      return BigInt(group.committedOffsets?.[partition] ?? '0');
    };

    // While the record is being retried, the group has not committed past it.
    await expect.poll(async () => (await queues.delivery(placement.messageId)).attempts, POLL).toBeGreaterThanOrEqual(1);
    expect(await committed(), 'not committed while retrying').toBeLessThanOrEqual(BigInt(placement.offset!));

    await waitForDelivery(queues, placement.messageId, 'delivered');
    await expect.poll(committed, { ...POLL, message: 'committed after the ack' }).toBeGreaterThan(BigInt(placement.offset!));
  });

  test('while a partition is blocked, the records behind it show up as consumer lag', async ({
    queues,
    cdp,
    eventStream,
  }) => {
    const placement = await queues.publishWithPlacement(eventStream.customerCreated());
    await waitForDelivery(queues, placement.messageId, 'delivered');
    const partition = placement.partition!;
    const lagOnPartition = async () =>
      (await queues.consumerGroups()).find((g) => g.consumerGroup === CDP_QUEUE)!.lagByPartition![partition]!;

    // The first earn is retried for 700 ms; the 5 behind it wait, uncommitted.
    await cdp.setFault(eventStream.customerId, 'fail-before-apply', 3);
    const ids = await queues.publishAll(Array.from({ length: 6 }, () => eventStream.earned(1)));
    await expect.poll(lagOnPartition, { intervals: [10, 25, 50], timeout: 2_000, message: 'lag while blocked' }).toBeGreaterThanOrEqual(5);

    await waitForDelivery(queues, ids.at(-1)!, 'delivered');
    expect((await cdp.profileOrNull(eventStream.customerId))?.attributes.points_balance, 'all 6 applied, in order').toBe(6);
    const group = (await queues.consumerGroups()).find((g) => g.consumerGroup === CDP_QUEUE)!;
    expect(group.lagByPartition, 'lag is reported per partition').toHaveLength(PARTITIONS);
  });
});

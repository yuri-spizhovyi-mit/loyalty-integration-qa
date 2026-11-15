import { uniqueCustomerId } from '../../src/builders/customer-builder.js';
import { anEventStream, type EventStream } from '../../src/builders/event-builder.js';
import { aRedeem, aRefundOf } from '../../src/builders/transaction-builder.js';
import type { LoyaltyEvent } from '../../src/clients/loyalty-types.js';
import { CDP_QUEUE, LOYALTY_TOPIC } from '../../src/config.js';
import { expect, test } from '../../src/fixtures.js';
import { deliverInOrder, waitForDelivery } from '../../src/helpers/delivery.js';
import { POLL } from '../../src/helpers/polling.js';
import type { QueueInspector } from '../../src/helpers/queue-inspector.js';
import { waitUntilInSync } from '../../src/helpers/sync.js';

/**
 * Behaviour that exists only on Kafka. The rest of the suite runs unchanged
 * on both buses; these tests check what Kafka adds or changes:
 *
 * - the key (customerId) decides the partition, and a partition keeps order;
 * - retries block their partition (head-of-line), not the others;
 * - offsets are committed only after a record is handled;
 * - a dead-letter record keeps the original bytes and says why it failed.
 */

const PARTITIONS = 3;

/** Creates synthetic customers until it has one on each wanted partition, keyed by role. */
async function customersOnPartitions(
  queues: QueueInspector,
  correlationId: string,
  wanted: { role: string; samePartitionAs?: string }[],
): Promise<Record<string, { stream: EventStream; partition: number }>> {
  const found: Record<string, { stream: EventStream; partition: number }> = {};
  for (let attempt = 0; attempt < 60 && Object.keys(found).length < wanted.length; attempt += 1) {
    const stream = anEventStream(uniqueCustomerId('kfk'), correlationId);
    const created = stream.customerCreated(true);
    const placement = await queues.publishWithPlacement(created);
    await waitForDelivery(queues, placement.messageId, 'delivered');
    const partition = placement.partition!;
    for (const { role, samePartitionAs } of wanted) {
      if (found[role]) continue;
      const anchor = samePartitionAs ? found[samePartitionAs] : undefined;
      const fits = samePartitionAs
        ? anchor !== undefined && anchor.partition === partition
        : Object.values(found).every((f) => f.partition !== partition);
      if (fits) {
        found[role] = { stream, partition };
        break;
      }
    }
  }
  expect(Object.keys(found).sort(), 'could not place customers on the partitions needed').toEqual(wanted.map((w) => w.role).sort());
  return found;
}

test.describe('Kafka: keys, partitions and ordering', () => {
  test("all of a customer's events go to one partition, in sequence order", async ({
    loyalty,
    cdp,
    queues,
    customer,
    earnPoints,
  }) => {
    const earn = await earnPoints(customer.customerId, 1200); // + tier.changed
    await expect(await loyalty.redeem(customer.customerId, aRedeem().points(100).build())).toHaveStatus(201);
    await expect(await loyalty.refund(customer.customerId, aRefundOf(earn.transaction.transactionId).points(300).build())).toHaveStatus(201);
    await waitUntilInSync(loyalty, cdp, customer.customerId);

    const records = await queues.messagesOn(LOYALTY_TOPIC, customer.customerId);
    expect(records).toHaveLength(6); // created, earned, tier up, redeemed, refunded, tier down
    expect(new Set(records.map((r) => r.partition)).size, 'one partition per key').toBe(1);
    const byOffset = [...records].sort((a, b) => Number(a.offset) - Number(b.offset));
    const sequences = byOffset.map((r) => (JSON.parse(r.payload) as LoyaltyEvent).sequence);
    expect(sequences, 'offset order is sequence order').toEqual([1, 2, 3, 4, 5, 6]);
  });

  test('different customers are spread over the partitions', async ({ queues, correlationId }) => {
    const partitions = new Set<number>();
    for (let i = 0; i < 12; i += 1) {
      const created = anEventStream(uniqueCustomerId('kfk'), correlationId).customerCreated();
      partitions.add((await queues.publishWithPlacement(created)).partition!);
    }
    // 12 random keys all on one of 3 partitions has a chance of about 1 in 177,000.
    expect(partitions.size).toBeGreaterThan(1);
    for (const partition of partitions) expect(partition).toBeLessThan(PARTITIONS);
  });
});

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

test.describe('Kafka: dead-letter topic records', () => {
  test('a rejected record goes to the DLT with its original bytes, key and placement', async ({
    queues,
    eventStream,
  }) => {
    const bad = { ...eventStream.customerCreated(), eventId: 'not-a-uuid' };
    const raw = JSON.stringify(bad);
    const placement = await queues.publishWithPlacement(raw);
    await waitForDelivery(queues, placement.messageId, 'dead-lettered');

    const [dead] = await queues.deadLetters({ messageId: placement.messageId });
    expect(dead?.deadLetterRecord).toMatchObject({
      topic: `${CDP_QUEUE}.dlt`,
      key: eventStream.customerId,
      payload: raw,
      headers: {
        'message-id': placement.messageId,
        'dlt-reason': 'rejected',
        'dlt-attempts': '1',
        'dlt-source-topic': LOYALTY_TOPIC,
        'dlt-source-partition': String(placement.partition),
        'dlt-source-offset': placement.offset,
      },
    });
    expect(dead!.deadLetterRecord!.headers['dlt-error']).toContain('eventId must be a UUID');
  });

  test('a poison record reaches the DLT after the attempt budget, with the count in its headers', async ({
    queues,
    cdp,
    eventStream,
  }) => {
    await deliverInOrder(queues, [eventStream.customerCreated()]);
    await cdp.setFault(eventStream.customerId, 'fail-before-apply', 1000);
    const messageId = await queues.publish(eventStream.earned(5));
    await waitForDelivery(queues, messageId, 'dead-lettered');

    const [dead] = await queues.deadLetters({ messageId });
    expect(dead?.deadLetterRecord?.headers).toMatchObject({
      'dlt-reason': 'max-attempts-exceeded',
      'dlt-attempts': '4',
    });
  });
});

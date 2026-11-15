import { uniqueCustomerId } from '../../src/builders/customer-builder.js';
import { anEventStream } from '../../src/builders/event-builder.js';
import { aRedeem, aRefundOf } from '../../src/builders/transaction-builder.js';
import type { LoyaltyEvent } from '../../src/clients/loyalty-types.js';
import { CDP_QUEUE, config, LOYALTY_TOPIC } from '../../src/config.js';
import { expect, test } from '../../src/fixtures.js';
import { deliverInOrder, waitForDelivery } from '../../src/helpers/delivery.js';
import { PARTITIONS } from '../../src/helpers/kafka-placement.js';
import { waitUntilInSync } from '../../src/helpers/sync.js';

/**
 * Behaviour that exists only on Kafka, in both retry modes. The rest of the
 * suite runs unchanged on every bus; these tests check what Kafka adds:
 *
 * - the key (customerId) decides the partition, and a partition keeps order;
 * - a dead-letter record keeps the original bytes and says why it failed,
 *   and where the record first came from.
 *
 * What differs between the retry modes is in blocking-retries.spec.ts and
 * retry-topics.spec.ts; only the file for the running mode is included.
 */

test('the consumers run in the retry mode the tests were started for', async ({ queues }) => {
  const groups = await queues.consumerGroups();
  expect(groups.map((g) => `${g.consumerGroup}: ${g.retryMode}`)).toEqual(
    groups.map((g) => `${g.consumerGroup}: ${config.kafkaRetryMode}`),
  );
});

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
    const [original] = (await queues.messagesOn(LOYALTY_TOPIC, eventStream.customerId)).filter((m) => m.messageId === messageId);
    // With retry topics the last attempt is read from retry-3, but the DLT
    // record must still point at the record on loyalty.events.
    expect(dead?.deadLetterRecord?.headers).toMatchObject({
      'dlt-reason': 'max-attempts-exceeded',
      'dlt-attempts': '4',
      'dlt-source-topic': LOYALTY_TOPIC,
      'dlt-source-partition': String(original!.partition),
      'dlt-source-offset': original!.offset,
    });
  });
});

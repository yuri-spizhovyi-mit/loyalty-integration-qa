import { invalidEvents } from '../../src/builders/invalid-events.js';
import { config } from '../../src/config.js';
import { expect, test } from '../../src/fixtures.js';
import { deliverInOrder, waitForDelivery } from '../../src/helpers/delivery.js';
import { POLL } from '../../src/helpers/polling.js';

/**
 * Retries and the dead-letter queue (DLQ).
 *
 * - The consumer answers 400 to a message it can never accept: the broker
 *   dead-letters it at once, without retrying.
 * - It answers 503 to a temporary failure: the broker retries with backoff
 *   and dead-letters after the last attempt.
 * Failures are injected per customer through the consumer's fault endpoint.
 */

const MAX_ATTEMPTS = config.brokerMaxAttempts;

test.describe('Dead-letter queue: messages the consumer can never accept', () => {
  test('malformed JSON is dead-lettered after one attempt, with the reason', async ({ queues }) => {
    const messageId = await queues.publish('{"eventId": "abc", ');

    const delivery = await waitForDelivery(queues, messageId, 'dead-lettered');

    expect(delivery).toMatchObject({ attempts: 1, deadLetterReason: 'rejected' });
    expect(delivery.lastError).toContain('MALFORMED_JSON');
    const [entry] = await queues.deadLetters({ messageId });
    expect(entry?.payload, 'the DLQ keeps the original payload for investigation').toBe('{"eventId": "abc", ');
  });

  const representative = ['unknown event type', 'points as a string', 'tier in lower case', 'no customerId'];
  for (const name of representative) {
    test(`an event with ${name} is dead-lettered without retries and changes no profile`, async ({
      cdp,
      queues,
      eventStream,
    }) => {
      const invalid = invalidEvents(eventStream).find((candidate) => candidate.name === name);
      expect(invalid, `invalid event case "${name}"`).toBeDefined();

      const delivery = await waitForDelivery(queues, await queues.publish(invalid!.body), 'dead-lettered');

      expect(delivery).toMatchObject({ attempts: 1, deadLetterReason: 'rejected' });
      expect(delivery.lastError).toContain(invalid!.code);
      expect(await cdp.profileOrNull(eventStream.customerId)).toBeNull();
    });
  }

  test('the DLQ can be filtered by customer and holds the correlation ID', async ({
    queues,
    eventStream,
    correlationId,
  }) => {
    const bad = { ...eventStream.customerCreated(), sequence: 2 };
    const messageId = await queues.publish(bad);
    await waitForDelivery(queues, messageId, 'dead-lettered');

    const entries = await queues.deadLetters({ customerId: eventStream.customerId });
    expect(entries.map((entry) => entry.messageId)).toEqual([messageId]);
    expect(entries[0]?.correlationId).toBe(correlationId);
  });
});

test.describe('Retries: temporary consumer failures', () => {
  test('a message that fails twice is delivered on the third attempt', async ({ cdp, queues, eventStream }) => {
    await expect(await cdp.setFault(eventStream.customerId, 'fail-before-apply', 2)).toHaveStatus(204);
    const created = eventStream.customerCreated();

    const delivery = await waitForDelivery(queues, await queues.publish(created), 'delivered');

    expect(delivery.attempts).toBe(3);
    expect(delivery.history.map(({ outcome, status }) => [outcome, status])).toEqual([
      ['retry', 503],
      ['retry', 503],
      ['ack', 200],
    ]);
    const profile = await cdp.profileOrNull(eventStream.customerId);
    expect(profile?.sync).toMatchObject({ appliedEvents: 1, duplicateEvents: 0 });
  });

  test('retries back off: each wait is longer than the one before', async ({ cdp, queues, eventStream }) => {
    await expect(await cdp.setFault(eventStream.customerId, 'fail-before-apply', 3)).toHaveStatus(204);

    const delivery = await waitForDelivery(queues, await queues.publish(eventStream.customerCreated()), 'delivered');

    const times = delivery.history.map((attempt) => Date.parse(attempt.at));
    const gaps = times.slice(1).map((time, index) => time - (times[index] as number));
    expect(gaps).toHaveLength(3);
    for (let i = 1; i < gaps.length; i += 1) expect(gaps[i]).toBeGreaterThan(gaps[i - 1] as number);
  });

  test(`a poison message is dead-lettered after ${MAX_ATTEMPTS} attempts and changes no profile`, async ({
    cdp,
    queues,
    eventStream,
  }) => {
    await expect(await cdp.setFault(eventStream.customerId, 'fail-before-apply', 100)).toHaveStatus(204);

    const delivery = await waitForDelivery(queues, await queues.publish(eventStream.customerCreated()), 'dead-lettered');

    expect(delivery).toMatchObject({ attempts: MAX_ATTEMPTS, deadLetterReason: 'max-attempts-exceeded' });
    expect(delivery.history.every((attempt) => attempt.outcome === 'retry')).toBe(true);
    expect(await cdp.profileOrNull(eventStream.customerId)).toBeNull();
  });

  test('a dead-lettered message is applied after the fault is fixed and it is redriven', async ({
    cdp,
    queues,
    eventStream,
  }) => {
    await expect(await cdp.setFault(eventStream.customerId, 'fail-before-apply', 100)).toHaveStatus(204);
    const messageId = await queues.publish(eventStream.customerCreated(true));
    await waitForDelivery(queues, messageId, 'dead-lettered');

    await expect(await cdp.clearFault(eventStream.customerId)).toHaveStatus(204);
    await queues.redrive(messageId);

    await waitForDelivery(queues, messageId, 'delivered');
    await expect
      .poll(async () => (await cdp.profileOrNull(eventStream.customerId))?.attributes, POLL)
      .toMatchObject({ loyalty_tier: 'bronze', marketing_consent: true });
    expect(await queues.deadLetters({ messageId })).toEqual([]);
  });

  test('a dead-lettered message does not block later messages, and the gap is visible', async ({
    cdp,
    queues,
    eventStream,
  }) => {
    const created = eventStream.customerCreated();
    const lost = eventStream.earned(100);
    const later = eventStream.earned(50);
    await deliverInOrder(queues, [created]);

    // Exactly enough failures to exhaust the retries of the next message.
    await expect(await cdp.setFault(eventStream.customerId, 'fail-before-apply', MAX_ATTEMPTS)).toHaveStatus(204);
    const lostId = await queues.publish(lost);
    await waitForDelivery(queues, lostId, 'dead-lettered');
    await deliverInOrder(queues, [later]);

    const withGap = await cdp.profileOrNull(eventStream.customerId);
    expect(withGap?.attributes.points_balance).toBe(150);
    expect(withGap?.sync.missingSequences, 'the profile shows which event is missing').toEqual([lost.sequence]);

    await queues.redrive(lostId);
    await waitForDelivery(queues, lostId, 'delivered');
    const repaired = await cdp.profileOrNull(eventStream.customerId);
    expect(repaired?.sync.missingSequences).toEqual([]);
    expect(repaired?.attributes.points_balance, 'the older event must not overwrite the newer balance').toBe(150);
  });
});

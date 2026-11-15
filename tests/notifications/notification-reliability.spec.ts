import { aProfileTierChanged } from '../../src/builders/profile-event-builder.js';
import { NOTIFICATION_QUEUE, PROFILE_TOPIC } from '../../src/config.js';
import { expect, test } from '../../src/fixtures.js';
import { deliverInOrder, waitForDelivery } from '../../src/helpers/delivery.js';
import { waitForDecisions } from '../../src/helpers/notification-wait.js';
import { POLL } from '../../src/helpers/polling.js';

/**
 * Exactly-once notifications on top of at-least-once delivery. These tests
 * skip the engine and publish to the broker directly: engine events on
 * loyalty.events (through the customer data platform), or profile events on
 * profile.events (straight to the notification service). That lets them
 * duplicate, reorder, drop and break events at will.
 */

test.describe('Duplicates and lost acknowledgements', () => {
  test('a tier.changed delivered twice to the CDP sends one notification', async ({
    queues,
    notifications,
    eventStream,
  }) => {
    const created = eventStream.customerCreated(true);
    const earned = eventStream.earned(1000);
    const upgrade = eventStream.tierChanged('Bronze', 'Silver', 1000, earned);
    await deliverInOrder(queues, [created, earned, upgrade, upgrade]);

    const result = await waitForDecisions(notifications, eventStream.customerId, [upgrade.eventId]);
    expect(result.notifications).toHaveLength(1);
    expect(result.decisions.map((d) => d.outcome)).toEqual(['sent']);
    // The duplicate was dropped by the CDP, so it never reached the second queue.
    expect(await queues.messagesOn(PROFILE_TOPIC, eventStream.customerId)).toHaveLength(1);
  });

  test('a lost ack at the CDP (applied, then 503) still sends one notification', async ({
    queues,
    cdp,
    notifications,
    eventStream,
  }) => {
    const created = eventStream.customerCreated(true);
    const earned = eventStream.earned(1000);
    const upgrade = eventStream.tierChanged('Bronze', 'Silver', 1000, earned);
    await deliverInOrder(queues, [created, earned]);
    await cdp.setFault(eventStream.customerId, 'fail-after-apply', 1);

    const messageId = await queues.publish(upgrade);
    const delivery = await waitForDelivery(queues, messageId, 'delivered');
    expect(delivery.attempts, 'the broker redelivered after the lost ack').toBe(2);

    const result = await waitForDecisions(notifications, eventStream.customerId, [upgrade.eventId]);
    expect(result.notifications).toHaveLength(1);
    expect(await queues.messagesOn(PROFILE_TOPIC, eventStream.customerId)).toHaveLength(1);
  });

  test('a lost ack at the notification service still records one notification', async ({
    queues,
    notifications,
    eventStream,
    correlationId,
  }) => {
    const event = aProfileTierChanged(eventStream.customerId, correlationId).build();
    await notifications.setFault(eventStream.customerId, 'fail-after-apply', 1);

    const messageId = await queues.publish(event, PROFILE_TOPIC);
    const delivery = await waitForDelivery(queues, messageId, 'delivered', NOTIFICATION_QUEUE);
    expect(delivery.attempts).toBe(2);

    const decisions = await notifications.decisions(eventStream.customerId);
    expect(decisions.map((d) => d.outcome)).toEqual(['sent', 'duplicate']);
    expect(decisions[1]!.notificationId, 'the duplicate points at the original').toBe(decisions[0]!.notificationId);
    expect(await notifications.notifications(eventStream.customerId)).toHaveLength(1);
  });

  test('the same tier change republished under a new eventId is still one notification', async ({
    queues,
    notifications,
    eventStream,
    correlationId,
  }) => {
    const original = aProfileTierChanged(eventStream.customerId, correlationId).build();
    const copy = aProfileTierChanged(eventStream.customerId, correlationId).copyOf(original).build();
    expect(copy.eventId).not.toBe(original.eventId);

    for (const event of [original, copy]) {
      await waitForDelivery(queues, await queues.publish(event, PROFILE_TOPIC), 'delivered', NOTIFICATION_QUEUE);
    }
    const decisions = await notifications.decisions(eventStream.customerId);
    expect(decisions.map((d) => `${d.eventId === copy.eventId ? 'copy' : 'original'} ${d.outcome}`)).toEqual([
      'original sent',
      'copy duplicate',
    ]);
    expect(await notifications.notifications(eventStream.customerId)).toHaveLength(1);
  });

  test('temporary failures are retried and end in exactly one notification', async ({
    queues,
    notifications,
    eventStream,
    correlationId,
  }) => {
    await notifications.setFault(eventStream.customerId, 'fail-before-apply', 2);

    const messageId = await queues.publish(aProfileTierChanged(eventStream.customerId, correlationId).build(), PROFILE_TOPIC);
    const delivery = await waitForDelivery(queues, messageId, 'delivered', NOTIFICATION_QUEUE);
    expect(delivery.history.map((h) => h.outcome)).toEqual(['retry', 'retry', 'ack']);
    expect(await notifications.decisions(eventStream.customerId)).toHaveLength(1);
    expect(await notifications.notifications(eventStream.customerId)).toHaveLength(1);
  });
});

test.describe('Ordering and consent', () => {
  test('a stale upgrade that arrives after a newer downgrade sends nothing', async ({
    queues,
    cdp,
    notifications,
    eventStream,
  }) => {
    const created = eventStream.customerCreated(true);
    const earned = eventStream.earned(1200);
    const upgrade = eventStream.tierChanged('Bronze', 'Silver', 1200, earned); // seq 3
    const refunded = eventStream.refunded(1200, earned);
    const downgrade = eventStream.tierChanged('Silver', 'Bronze', 0, refunded); // seq 5
    await deliverInOrder(queues, [created, earned, refunded, downgrade, upgrade]);

    await expect.poll(async () => (await cdp.profileOrNull(eventStream.customerId))?.sync.appliedEvents, POLL).toBe(5);
    const result = await waitForDecisions(notifications, eventStream.customerId, [downgrade.eventId]);
    // The profile went straight to its current state; the stale upgrade is history.
    expect(result.notifications.map((n) => n.template)).toEqual(['tier-downgrade']);
    expect((await queues.messagesOn(PROFILE_TOPIC, eventStream.customerId)).length).toBe(1);
  });

  test('a tier change before customer.created has unknown consent and sends nothing, even later', async ({
    queues,
    notifications,
    eventStream,
  }) => {
    const created = eventStream.customerCreated(true);
    const earned = eventStream.earned(1000);
    const upgrade = eventStream.tierChanged('Bronze', 'Silver', 1000, earned);
    await deliverInOrder(queues, [earned, upgrade]);

    const result = await waitForDecisions(notifications, eventStream.customerId, [upgrade.eventId]);
    expect(result.decisions.map((d) => d.outcome)).toEqual(['suppressed-consent-unknown']);

    // Consent arriving afterwards does not send the old message retroactively.
    await deliverInOrder(queues, [created]);
    expect(await notifications.decisions(eventStream.customerId)).toHaveLength(1);
    expect(await notifications.notifications(eventStream.customerId)).toEqual([]);
  });
});

test.describe('Dead-letter queue for the notification service', () => {
  const poison: [string, string][] = [
    ['malformed JSON', '{"type": "profile.tier_changed",'],
    ['an engine event on the profile topic', JSON.stringify({ type: 'tier.changed', customerId: 'qa-dlq-1' })],
  ];
  for (const [name, payload] of poison) {
    test(`${name} is dead-lettered without retries`, async ({ queues }) => {
      const messageId = await queues.publish(payload, PROFILE_TOPIC);
      const delivery = await waitForDelivery(queues, messageId, 'dead-lettered', NOTIFICATION_QUEUE);
      expect(delivery).toMatchObject({ attempts: 1, deadLetterReason: 'rejected' });
      expect(await queues.deadLetters({ messageId }, NOTIFICATION_QUEUE)).toHaveLength(1);
    });
  }

  test('a dead-lettered profile event can be redriven once the cause is fixed', async ({
    queues,
    notifications,
    eventStream,
    correlationId,
  }) => {
    // A long outage: more failures than the broker's attempt budget.
    await notifications.setFault(eventStream.customerId, 'fail-before-apply', 1000);
    const messageId = await queues.publish(aProfileTierChanged(eventStream.customerId, correlationId).build(), PROFILE_TOPIC);
    const dead = await waitForDelivery(queues, messageId, 'dead-lettered', NOTIFICATION_QUEUE);
    expect(dead.deadLetterReason).toBe('max-attempts-exceeded');
    expect(await notifications.notifications(eventStream.customerId)).toEqual([]);

    await notifications.clearFault(eventStream.customerId);
    await queues.redrive(messageId, NOTIFICATION_QUEUE);
    await waitForDelivery(queues, messageId, 'delivered', NOTIFICATION_QUEUE);
    expect(await notifications.notifications(eventStream.customerId)).toHaveLength(1);
  });
});

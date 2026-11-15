import { CDP_QUEUE } from '../config.js';
import type { DeliveryStatus, Delivery, QueueInspector } from './queue-inspector.js';
import { expect } from './matchers.js';
import { DEAD_LETTER_TIMEOUT_MS, POLL } from './polling.js';

/** Polls the broker until the message leaves "pending" and checks where it ended up. */
export async function waitForDelivery(
  queues: QueueInspector,
  messageId: string,
  expected: Exclude<DeliveryStatus, 'pending'>,
  queue = CDP_QUEUE,
): Promise<Delivery> {
  await expect
    .poll(async () => (await queues.delivery(messageId, queue)).status, {
      message: `broker delivery of ${messageId} to ${queue}`,
      intervals: POLL.intervals,
      timeout: DEAD_LETTER_TIMEOUT_MS,
    })
    .not.toBe('pending');
  const delivery = await queues.delivery(messageId, queue);
  expect(delivery.status, `delivery of ${messageId}: last error ${delivery.lastError ?? '(none)'}`).toBe(expected);
  return delivery;
}

/**
 * Publishes events one at a time and waits for each to be delivered before
 * the next, so the consumer sees them in exactly this order.
 */
export async function deliverInOrder(queues: QueueInspector, events: unknown[]): Promise<string[]> {
  const ids: string[] = [];
  for (const event of events) {
    const messageId = await queues.publish(event);
    await waitForDelivery(queues, messageId, 'delivered');
    ids.push(messageId);
  }
  return ids;
}

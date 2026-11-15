import type { LoyaltyClient } from '../clients/loyalty-client.js';
import type { NotificationClient } from '../clients/notification-client.js';
import type { Decision, Notification } from '../clients/notification-types.js';
import type { OutboxResponse } from '../clients/loyalty-types.js';
import { readJson } from './json.js';
import { expect } from './matchers.js';
import { POLL, SYNC_SLA_MS } from './polling.js';

/**
 * The full path from the engine to a notification crosses two queues, so it
 * gets twice the single-hop sync time.
 */
export const NOTIFICATION_SLA_MS = 2 * SYNC_SLA_MS;

/**
 * Polls until the notification service has decided on every given engine
 * tier.changed event (by sourceEventId), then returns the decisions and
 * notifications. Proving "no notification" works the same way: wait for
 * the decision, then look.
 */
export async function waitForDecisions(
  notifications: NotificationClient,
  customerId: string,
  sourceEventIds: string[],
): Promise<{ decisions: Decision[]; notifications: Notification[] }> {
  await expect
    .poll(
      async () => {
        const decided = new Set((await notifications.decisions(customerId)).map((d) => d.sourceEventId));
        return sourceEventIds.filter((id) => !decided.has(id));
      },
      { ...POLL, timeout: NOTIFICATION_SLA_MS, message: `customer ${customerId}: tier changes still without a decision` },
    )
    .toEqual([]);
  return {
    decisions: await notifications.decisions(customerId),
    notifications: await notifications.notifications(customerId),
  };
}

/** The engine's tier.changed eventIds for a customer, oldest first. */
export async function tierChangeIds(loyalty: LoyaltyClient, customerId: string): Promise<string[]> {
  const { events } = await readJson<OutboxResponse>(await loyalty.getOutbox(customerId));
  return events
    .filter((event) => event.type === 'tier.changed')
    .sort((a, b) => a.sequence - b.sequence)
    .map((event) => event.eventId);
}

/** Waits for decisions on all of the customer's tier changes in the engine so far. */
export async function settleNotifications(loyalty: LoyaltyClient, notifications: NotificationClient, customerId: string) {
  const ids = await tierChangeIds(loyalty, customerId);
  return { tierChangeIds: ids, ...(await waitForDecisions(notifications, customerId, ids)) };
}

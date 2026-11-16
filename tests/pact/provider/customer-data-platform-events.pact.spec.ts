import { MessageProviderPact } from '@pact-foundation/pact';
import { test } from '@playwright/test';
import { eventHandler } from '../../../services/customer-data-platform/src/app.js';
import { FaultInjector } from '../../../services/customer-data-platform/src/faults.js';
import { ProfileStore } from '../../../services/customer-data-platform/src/profiles.js';
import type { ProfileTierChanged } from '../../../services/customer-data-platform/src/publisher.js';
import type { LoyaltyEvent } from '../../../services/loyalty-engine/src/events.js';
import { pactFile, PARTICIPANTS } from '../../../src/pact/contracts.js';
import { earn, engineEvents } from '../../../src/pact/engine-scenarios.js';

/**
 * Verifies the customer data platform's `profile.tier_changed` against what
 * the notification service expects. The input is real: the engine's own
 * events (from an in-process engine) go through the platform's real handler,
 * and whatever it publishes is the message under test.
 */

/** Feeds the engine's events to a fresh CDP and returns the profile.tier_changed it published. */
function cdpPublishes(events: LoyaltyEvent[]): ProfileTierChanged {
  const published: ProfileTierChanged[] = [];
  const handle = eventHandler(new ProfileStore(), new FaultInjector(), { publish: (event) => published.push(event) });
  for (const event of events) {
    const { status } = handle(JSON.stringify(event), { messageId: event.eventId, correlationId: event.correlationId });
    if (status !== 200) throw new Error(`the CDP answered ${status} to ${event.type}`);
  }
  if (published.length !== 1) throw new Error(`expected one profile.tier_changed, got ${published.length}`);
  return published[0]!;
}

const reachSilver = (marketingConsent: boolean) =>
  engineEvents((engine, id, corr) => earn(engine, id, corr, 1000), { marketingConsent });

test('the customer data platform publishes the profile events the notification service relies on', async () => {
  test.setTimeout(60_000);
  await new MessageProviderPact({
    provider: PARTICIPANTS.cdp,
    pactUrls: [pactFile(PARTICIPANTS.notifications, PARTICIPANTS.cdp)],
    logLevel: 'warn',
    messageProviders: {
      'a profile.tier_changed event for a customer who consented': async () => cdpPublishes(await reachSilver(true)),
      'a profile.tier_changed event for a customer who did not consent': async () => cdpPublishes(await reachSilver(false)),
      // customer.created is still on its way: only the later events have arrived.
      'a profile.tier_changed event before the customer.created event arrived': async () =>
        cdpPublishes((await reachSilver(true)).filter((event) => event.type !== 'customer.created')),
    },
  }).verify();
});

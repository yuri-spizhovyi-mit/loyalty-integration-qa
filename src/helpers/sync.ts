import type { CdpClient } from '../clients/cdp-client.js';
import type { Profile } from '../clients/cdp-types.js';
import type { LoyaltyClient } from '../clients/loyalty-client.js';
import type { OutboxResponse } from '../clients/loyalty-types.js';
import { readJson } from './json.js';
import { expect } from './matchers.js';
import { POLL } from './polling.js';

/** Polls until the profile has applied every event the engine published for this customer. */
export async function waitUntilInSync(loyalty: LoyaltyClient, cdp: CdpClient, customerId: string): Promise<Profile> {
  const outbox = await readJson<OutboxResponse>(await loyalty.getOutbox(customerId));
  const published = outbox.events.length;
  await expect
    .poll(async () => (await cdp.profileOrNull(customerId))?.sync, {
      ...POLL,
      message: `profile ${customerId} should have applied all ${published} published events`,
    })
    .toMatchObject({ lastSequence: published, appliedEvents: published, missingSequences: [] });
  return (await cdp.profileOrNull(customerId)) as Profile;
}

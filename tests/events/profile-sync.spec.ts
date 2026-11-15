import { aCustomer } from '../../src/builders/customer-builder.js';
import { anEarn, aRedeem, aRefundOf, uniqueIdempotencyKey } from '../../src/builders/transaction-builder.js';
import type { CustomerResponse, LedgerResponse } from '../../src/clients/loyalty-types.js';
import { expect, test } from '../../src/fixtures.js';
import { readJson } from '../../src/helpers/json.js';
import { POLL, SYNC_SLA_MS } from '../../src/helpers/polling.js';
import { waitUntilInSync } from '../../src/helpers/sync.js';

/**
 * End to end: REST call on the engine -> event -> broker -> customer data
 * platform profile. Every check polls; nothing sleeps for a fixed time.
 */

test.describe('Profile sync: engine -> broker -> customer data platform', () => {
  test(`a new member's profile appears within ${SYNC_SLA_MS} ms as Bronze with a zero balance`, async ({
    cdp,
    createCustomer,
  }) => {
    const { customerId } = await createCustomer(aCustomer().withMarketingConsent(true).build());

    await expect
      .poll(() => cdp.profileOrNull(customerId), { ...POLL, message: `profile ${customerId}` })
      .toMatchObject({
        profileId: customerId,
        attributes: { loyalty_tier: 'bronze', points_balance: 0, marketing_consent: true },
      });
  });

  test('an earn updates the balance and the last transaction', async ({ cdp, customer, earnPoints }) => {
    const { transaction } = await earnPoints(customer.customerId, 250);

    await expect
      .poll(async () => (await cdp.profileOrNull(customer.customerId))?.attributes, POLL)
      .toMatchObject({
        points_balance: 250,
        last_transaction_id: transaction.transactionId,
        last_transaction_type: 'earn',
      });
  });

  test('tier upgrades reach the profile as lower-case tier names', async ({ cdp, customer, earnPoints }) => {
    const tierOf = async () => (await cdp.profileOrNull(customer.customerId))?.attributes.loyalty_tier;

    await earnPoints(customer.customerId, 1000);
    await expect.poll(tierOf, { ...POLL, message: 'after 1000 points' }).toBe('silver');

    await earnPoints(customer.customerId, 4000);
    await expect.poll(tierOf, { ...POLL, message: 'after 5000 points' }).toBe('gold');
  });

  test('after earn, redeem and refund the profile matches the engine', async ({ loyalty, cdp, customer, earnPoints }) => {
    const { customerId } = customer;
    const earn = await earnPoints(customerId, 1200);
    await expect(await loyalty.redeem(customerId, aRedeem().points(100).build())).toHaveStatus(201);
    await expect(
      await loyalty.refund(customerId, aRefundOf(earn.transaction.transactionId).points(300).build()),
    ).toHaveStatus(201);

    const profile = await waitUntilInSync(loyalty, cdp, customerId);
    const engine = await readJson<CustomerResponse>(await loyalty.getCustomer(customerId));
    const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(customerId));
    const lastEntry = ledger.entries.at(-1);

    expect(profile.attributes).toMatchObject({
      points_balance: engine.balance,
      loyalty_tier: engine.tier.toLowerCase(),
      last_transaction_id: lastEntry?.transactionId,
      last_transaction_type: lastEntry?.type.toLowerCase(),
    });
    expect(profile.sync.duplicateEvents).toBe(0);
  });

  test('an idempotent replay reaches the profile once', async ({ loyalty, cdp, customer, earnPoints }) => {
    const key = uniqueIdempotencyKey();
    const request = anEarn().points(100).build();
    await expect(await loyalty.earn(customer.customerId, request, { idempotencyKey: key })).toHaveStatus(201);
    await expect(await loyalty.earn(customer.customerId, request, { idempotencyKey: key })).toHaveStatus(201);
    // A later write as a marker: once it is in the profile, anything the
    // replay might have published would have arrived too.
    await earnPoints(customer.customerId, 1);

    const profile = await waitUntilInSync(loyalty, cdp, customer.customerId);
    expect(profile.sync.appliedEvents).toBe(3); // customer.created, earn, marker earn
    expect(profile.attributes.points_balance).toBe(101);
  });

  test('a rejected transaction does not change the profile', async ({ loyalty, cdp, customer, earnPoints }) => {
    await earnPoints(customer.customerId, 100);
    await expect(await loyalty.redeem(customer.customerId, aRedeem().points(500).build())).toBeApiError(
      422,
      'INSUFFICIENT_BALANCE',
    );
    await earnPoints(customer.customerId, 1);

    const profile = await waitUntilInSync(loyalty, cdp, customer.customerId);
    expect(profile.sync.appliedEvents).toBe(3);
    expect(profile.attributes).toMatchObject({ points_balance: 101, last_transaction_type: 'earn' });
  });

  test('parallel writes for one customer all reach the profile, in any arrival order', async ({
    loyalty,
    cdp,
    customer,
  }) => {
    const responses = await Promise.all(
      Array.from({ length: 10 }, () => loyalty.earn(customer.customerId, anEarn().points(10).build())),
    );
    for (const response of responses) await expect(response).toHaveStatus(201);

    const profile = await waitUntilInSync(loyalty, cdp, customer.customerId);
    expect(profile.attributes.points_balance).toBe(100);
    expect(profile.sync.appliedEvents).toBe(11);
  });

  test('profiles of different customers do not mix', async ({ loyalty, cdp, createCustomer, earnPoints }) => {
    const first = await createCustomer();
    const second = await createCustomer(aCustomer().withMarketingConsent(true).build());
    await Promise.all([earnPoints(first.customerId, 1500), earnPoints(second.customerId, 30)]);

    const [one, two] = await Promise.all([
      waitUntilInSync(loyalty, cdp, first.customerId),
      waitUntilInSync(loyalty, cdp, second.customerId),
    ]);
    expect(one.attributes).toMatchObject({ points_balance: 1500, loyalty_tier: 'silver', marketing_consent: false });
    expect(two.attributes).toMatchObject({ points_balance: 30, loyalty_tier: 'bronze', marketing_consent: true });
  });
});

import type { CustomerResponse, Tier, TierResponse } from '../../src/clients/loyalty-types.js';
import { expect, test } from '../../src/fixtures.js';
import { readJson } from '../../src/helpers/json.js';

// Bronze 0-999, Silver 1000-4999, Gold 5000+. Each threshold is checked at -1 and exactly.
const boundaries: { points: number; tier: Tier; nextTier: Tier | null; pointsToNextTier: number | null }[] = [
  { points: 1, tier: 'Bronze', nextTier: 'Silver', pointsToNextTier: 999 },
  { points: 999, tier: 'Bronze', nextTier: 'Silver', pointsToNextTier: 1 },
  { points: 1000, tier: 'Silver', nextTier: 'Gold', pointsToNextTier: 4000 },
  { points: 4999, tier: 'Silver', nextTier: 'Gold', pointsToNextTier: 1 },
  { points: 5000, tier: 'Gold', nextTier: null, pointsToNextTier: null },
  { points: 250_000, tier: 'Gold', nextTier: null, pointsToNextTier: null },
];

test.describe('Tier lookup', () => {
  test('a new customer is Bronze, 1000 points away from Silver', async ({ loyalty, customer }) => {
    const response = await loyalty.getTier(customer.customerId);

    await expect(response).toHaveStatus(200);
    expect(await readJson<TierResponse>(response)).toEqual({
      customerId: customer.customerId,
      tier: 'Bronze',
      qualifyingPoints: 0,
      nextTier: 'Silver',
      pointsToNextTier: 1000,
    });
  });

  for (const { points, tier, nextTier, pointsToNextTier } of boundaries) {
    test(`${points} qualifying points -> ${tier}`, async ({ loyalty, customer, earnPoints }) => {
      await earnPoints(customer.customerId, points);

      const response = await loyalty.getTier(customer.customerId);

      await expect(response).toHaveStatus(200);
      expect(await readJson<TierResponse>(response)).toMatchObject({
        tier,
        qualifyingPoints: points,
        nextTier,
        pointsToNextTier,
      });
    });
  }

  test('crossing a threshold by one point upgrades the tier', async ({ customer, earnPoints }) => {
    const before = await earnPoints(customer.customerId, 999);
    const after = await earnPoints(customer.customerId, 1);

    expect(before.tier).toBe('Bronze');
    expect(after.tier).toBe('Silver');
  });

  test('one earn can skip a tier: Bronze straight to Gold', async ({ customer, earnPoints }) => {
    const result = await earnPoints(customer.customerId, 6000);

    expect(result.tier).toBe('Gold');
  });

  test('the customer endpoint and the tier endpoint agree', async ({ loyalty, customer, earnPoints }) => {
    await earnPoints(customer.customerId, 1500);

    const profile = await readJson<CustomerResponse>(await loyalty.getCustomer(customer.customerId));
    const tier = await readJson<TierResponse>(await loyalty.getTier(customer.customerId));

    expect(profile.tier).toBe(tier.tier);
    expect(profile.qualifyingPoints).toBe(tier.qualifyingPoints);
  });
});

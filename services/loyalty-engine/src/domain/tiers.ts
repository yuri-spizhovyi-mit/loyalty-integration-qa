/**
 * Tier rules. A tier is based on qualifying points: everything ever earned,
 * minus refunds. Redeeming points spends the balance but does not lower the
 * tier; a refund takes the purchase back, so it can.
 */
export const TIERS = [
  { name: 'Bronze', minPoints: 0 },
  { name: 'Silver', minPoints: 1000 },
  { name: 'Gold', minPoints: 5000 },
] as const;

export type TierName = (typeof TIERS)[number]['name'];

export function tierFor(qualifyingPoints: number): TierName {
  let current: TierName = 'Bronze';
  for (const tier of TIERS) {
    if (qualifyingPoints >= tier.minPoints) current = tier.name;
  }
  return current;
}

export function tierRank(tier: TierName): number {
  return TIERS.findIndex((t) => t.name === tier);
}

export function nextTierFor(qualifyingPoints: number): { name: TierName; pointsNeeded: number } | null {
  const next = TIERS.find((tier) => tier.minPoints > qualifyingPoints);
  return next ? { name: next.name, pointsNeeded: next.minPoints - qualifyingPoints } : null;
}

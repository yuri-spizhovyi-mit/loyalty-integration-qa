import type {
  LedgerEntry,
  LedgerResponse,
  LoyaltyEvent,
  Tier,
  TierChangedData,
  TransactionEventData,
} from '../clients/loyalty-types.js';

/**
 * Data integrity checks. Each function returns every broken rule as a
 * readable line instead of stopping at the first one, so one failed test
 * shows the whole picture: `expect(ledgerProblems(ledger)).toEqual([])`.
 *
 * The tier thresholds are written here from the business rules (README and
 * CLAUDE.md), not taken from the engine, so the checks stay independent.
 */

const TIER_THRESHOLDS: [Tier, number][] = [
  ['Gold', 5000],
  ['Silver', 1000],
  ['Bronze', 0],
];

export function expectedTier(qualifyingPoints: number): Tier {
  return TIER_THRESHOLDS.find(([, min]) => qualifyingPoints >= min)![0];
}

/** Earned minus refunded. Redeeming spends points but does not lower the tier. */
export function qualifyingPointsOf(entries: LedgerEntry[]): number {
  return entries.filter((entry) => entry.type !== 'REDEEM').reduce((sum, entry) => sum + entry.points, 0);
}

/** Rules every ledger must follow on its own. */
export function ledgerProblems(ledger: LedgerResponse): string[] {
  const problems: string[] = [];
  const { entries } = ledger;
  const sum = entries.reduce((total, entry) => total + entry.points, 0);
  if (sum !== ledger.balance) problems.push(`sum of entries is ${sum} but balance is ${ledger.balance}`);

  const seen = new Set<string>();
  const refunded = new Map<string, number>();
  let running = 0;
  entries.forEach((entry, index) => {
    const at = `entry ${index} (${entry.transactionId})`;
    if (seen.has(entry.transactionId)) problems.push(`${at}: transactionId appears twice`);
    seen.add(entry.transactionId);
    if (entry.customerId !== ledger.customerId) problems.push(`${at}: belongs to ${entry.customerId}`);
    if (!Number.isInteger(entry.points) || entry.points === 0) problems.push(`${at}: points ${entry.points} is not a non-zero integer`);
    if ((entry.type === 'EARN') !== entry.points > 0) problems.push(`${at}: ${entry.type} has points ${entry.points}`);

    running += entry.points;
    if (entry.balanceAfter !== running) problems.push(`${at}: balanceAfter ${entry.balanceAfter}, running total ${running}`);
    if (entry.balanceAfter < 0) problems.push(`${at}: balance went negative (${entry.balanceAfter})`);

    if (index > 0 && entry.createdAt < entries[index - 1]!.createdAt) problems.push(`${at}: createdAt goes backwards`);

    if (entry.type === 'REFUND') {
      const original = entries.slice(0, index).find((e) => e.transactionId === entry.refundOf);
      if (!original) problems.push(`${at}: refundOf ${entry.refundOf} is not an earlier entry`);
      else if (original.type !== 'EARN') problems.push(`${at}: refunds a ${original.type}`);
      else {
        const total = (refunded.get(original.transactionId) ?? 0) - entry.points;
        refunded.set(original.transactionId, total);
        if (total > original.points) problems.push(`${at}: ${total} refunded against an earn of ${original.points}`);
      }
    } else if (entry.refundOf !== null) {
      problems.push(`${at}: ${entry.type} has refundOf ${entry.refundOf}`);
    }
  });
  return problems;
}

/**
 * The engine's outbox against its ledger: every entry was published exactly
 * once with the same values, nothing else was published, sequences have no
 * gaps, and tier.changed appears exactly where the tier moved.
 */
export function outboxProblems(entries: LedgerEntry[], events: LoyaltyEvent[]): string[] {
  const problems: string[] = [];

  const sequences = events.map((event) => event.sequence).sort((a, b) => a - b);
  const expected = Array.from({ length: events.length }, (_, i) => i + 1);
  if (sequences.join() !== expected.join()) problems.push(`sequences are ${sequences.join()}, expected 1..${events.length}`);
  const ids = new Set(events.map((event) => event.eventId));
  if (ids.size !== events.length) problems.push('an eventId appears twice');

  const created = events.filter((event) => event.type === 'customer.created');
  if (created.length !== 1 || created[0]!.sequence !== 1) problems.push('expected one customer.created with sequence 1');

  const transactionEvents = events.filter((event) => event.type.startsWith('points.')) as unknown as LoyaltyEvent<TransactionEventData>[];
  if (transactionEvents.length !== entries.length) {
    problems.push(`${entries.length} ledger entries but ${transactionEvents.length} points.* events`);
  }
  for (const entry of entries) {
    const matching = transactionEvents.filter((event) => event.data.transactionId === entry.transactionId);
    if (matching.length !== 1) {
      problems.push(`${entry.transactionId}: published ${matching.length} times`);
      continue;
    }
    const { data } = matching[0]!;
    for (const field of ['points', 'balanceAfter', 'reference', 'refundOf'] as const) {
      if (data[field] !== entry[field]) problems.push(`${entry.transactionId}: event ${field} ${data[field]}, ledger ${entry[field]}`);
    }
    if (data.entryType !== entry.type) problems.push(`${entry.transactionId}: event entryType ${data.entryType}, ledger ${entry.type}`);
  }

  // Replay the ledger: a tier.changed must exist exactly for the entries that moved the tier.
  const tierEvents = events.filter((event) => event.type === 'tier.changed') as unknown as LoyaltyEvent<TierChangedData>[];
  const expectedChanges: string[] = [];
  let tier: Tier = 'Bronze';
  entries.forEach((entry, index) => {
    const next = expectedTier(qualifyingPointsOf(entries.slice(0, index + 1)));
    if (next !== tier) expectedChanges.push(`${entry.transactionId} ${tier}->${next}`);
    tier = next;
  });
  const actualChanges = tierEvents
    .sort((a, b) => a.sequence - b.sequence)
    .map((event) => `${event.data.transactionId} ${event.data.previousTier}->${event.data.newTier}`);
  if (actualChanges.join('\n') !== expectedChanges.join('\n')) {
    problems.push(`tier.changed events [${actualChanges.join(', ')}], expected [${expectedChanges.join(', ')}]`);
  }
  for (const change of tierEvents) {
    const cause = transactionEvents.find((event) => event.data.transactionId === change.data.transactionId);
    if (cause && change.sequence !== cause.sequence + 1) {
      problems.push(`tier.changed ${change.sequence} does not directly follow its transaction event ${cause.sequence}`);
    }
  }
  return problems;
}

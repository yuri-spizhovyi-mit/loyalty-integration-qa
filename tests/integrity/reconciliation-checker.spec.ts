import { randomUUID } from 'node:crypto';
import type { LedgerEntry, LedgerResponse, LoyaltyEvent } from '../../src/clients/loyalty-types.js';
import { expect, test } from '../../src/fixtures.js';
import { ledgerProblems, outboxProblems } from '../../src/helpers/reconciliation.js';

/**
 * Self-test of the reconciliation checks, with no service calls. A checker
 * that never finds anything would make every integrity test pass, so each
 * rule is shown to catch a ledger or outbox that breaks it.
 */

const customerId = 'qa-checker-1';
const txn = () => `txn_${randomUUID()}`;
let clock = Date.parse('2026-11-14T10:00:00.000Z');

function entry(type: LedgerEntry['type'], points: number, balanceAfter: number, refundOf: string | null = null): LedgerEntry {
  clock += 1000;
  return {
    transactionId: txn(),
    customerId,
    type,
    points,
    balanceAfter,
    reference: null,
    refundOf,
    correlationId: 'qa-corr',
    createdAt: new Date(clock).toISOString(),
  };
}

/** Earn 1200 (-> Silver), redeem 200, refund 500 of the earn (-> Bronze). */
function validLedger(): LedgerResponse {
  const earn = entry('EARN', 1200, 1200);
  return {
    customerId,
    balance: 500,
    entries: [earn, entry('REDEEM', -200, 1000), entry('REFUND', -500, 500, earn.transactionId)],
  };
}

function eventsFor(entries: LedgerEntry[]): LoyaltyEvent[] {
  let sequence = 1;
  const event = (type: LoyaltyEvent['type'], data: Record<string, unknown>): LoyaltyEvent => ({
    eventId: randomUUID(),
    type,
    sequence: sequence++,
    customerId,
    correlationId: 'qa-corr',
    occurredAt: new Date(clock).toISOString(),
    data,
  });
  const events = [event('customer.created', { tier: 'Bronze', marketingConsent: false })];
  const [earn, redeem, refund] = entries as [LedgerEntry, LedgerEntry, LedgerEntry];
  const transaction = (e: LedgerEntry) => ({
    transactionId: e.transactionId,
    entryType: e.type,
    points: e.points,
    balanceAfter: e.balanceAfter,
    reference: e.reference,
    refundOf: e.refundOf,
  });
  events.push(event('points.earned', transaction(earn)));
  events.push(event('tier.changed', { previousTier: 'Bronze', newTier: 'Silver', direction: 'upgrade', qualifyingPoints: 1200, transactionId: earn.transactionId }));
  events.push(event('points.redeemed', transaction(redeem)));
  events.push(event('points.refunded', transaction(refund)));
  events.push(event('tier.changed', { previousTier: 'Silver', newTier: 'Bronze', direction: 'downgrade', qualifyingPoints: 700, transactionId: refund.transactionId }));
  return events;
}

test.describe('Reconciliation checker self-test', () => {
  test('a consistent ledger and outbox have no problems', () => {
    const ledger = validLedger();
    expect(ledgerProblems(ledger)).toEqual([]);
    expect(outboxProblems(ledger.entries, eventsFor(ledger.entries))).toEqual([]);
  });

  const brokenLedgers: { name: string; breakIt: (ledger: LedgerResponse) => void; problem: RegExp }[] = [
    { name: 'balance differs from the sum', breakIt: (l) => (l.balance = 501), problem: /sum of entries is 500 but balance is 501/ },
    { name: 'a wrong balanceAfter', breakIt: (l) => (l.entries[1]!.balanceAfter = 999), problem: /balanceAfter 999, running total 1000/ },
    { name: 'a duplicated entry', breakIt: (l) => l.entries.push({ ...l.entries[1]! }), problem: /appears twice/ },
    { name: 'a REDEEM with positive points', breakIt: (l) => (l.entries[1]!.points = 200), problem: /REDEEM has points 200/ },
    { name: 'a refund of an unknown earn', breakIt: (l) => (l.entries[2]!.refundOf = txn()), problem: /is not an earlier entry/ },
    {
      name: 'refunds above the original earn',
      breakIt: (l) => {
        const extra = entry('REFUND', -800, -300, l.entries[0]!.transactionId);
        l.entries.push(extra);
        l.balance = -300;
      },
      problem: /1300 refunded against an earn of 1200/,
    },
    { name: 'a negative balance', breakIt: (l) => { l.entries.push(entry('REDEEM', -600, -100)); l.balance = -100; }, problem: /balance went negative/ },
    { name: "another customer's entry", breakIt: (l) => (l.entries[1]!.customerId = 'qa-someone-else'), problem: /belongs to qa-someone-else/ },
    { name: 'createdAt going backwards', breakIt: (l) => (l.entries[2]!.createdAt = '2026-01-01T00:00:00.000Z'), problem: /createdAt goes backwards/ },
  ];
  for (const { name, breakIt, problem } of brokenLedgers) {
    test(`ledger check catches ${name}`, () => {
      const ledger = validLedger();
      breakIt(ledger);
      expect(ledgerProblems(ledger).join('\n')).toMatch(problem);
    });
  }

  const brokenOutboxes: { name: string; breakIt: (events: LoyaltyEvent[]) => LoyaltyEvent[]; problem: RegExp }[] = [
    { name: 'a lost event', breakIt: (e) => e.filter((x) => x.type !== 'points.redeemed'), problem: /3 ledger entries but 2 points/ },
    { name: 'an event published twice', breakIt: (e) => [...e, { ...e[3]!, eventId: randomUUID(), sequence: 7 }], problem: /published 2 times/ },
    { name: 'a sequence gap', breakIt: (e) => e.map((x) => (x.sequence === 6 ? { ...x, sequence: 9 } : x)), problem: /sequences are 1,2,3,4,5,9/ },
    { name: 'an event whose values differ', breakIt: (e) => e.map((x) => (x.sequence === 4 ? { ...x, data: { ...x.data, balanceAfter: 1 } } : x)), problem: /event balanceAfter 1, ledger 1000/ },
    { name: 'a missing tier.changed', breakIt: (e) => e.filter((x) => x.sequence !== 6), problem: /expected \[.*Bronze->Silver, .*Silver->Bronze\]/ },
    { name: 'a tier.changed out of place', breakIt: (e) => e.map((x) => (x.sequence === 3 ? { ...x, sequence: 4 } : x.sequence === 4 ? { ...x, sequence: 3 } : x)), problem: /does not directly follow/ },
  ];
  for (const { name, breakIt, problem } of brokenOutboxes) {
    test(`outbox check catches ${name}`, () => {
      const ledger = validLedger();
      expect(outboxProblems(ledger.entries, breakIt(eventsFor(ledger.entries))).join('\n')).toMatch(problem);
    });
  }
});

import { anEarn, aRedeem, aRefundOf, uniqueIdempotencyKey } from '../../src/builders/transaction-builder.js';
import type { LoyaltyClient } from '../../src/clients/loyalty-client.js';
import type {
  CustomerResponse,
  LedgerEntry,
  LedgerResponse,
  OutboxResponse,
  TransactionResponse,
} from '../../src/clients/loyalty-types.js';
import { expect, test } from '../../src/fixtures.js';
import { readJson } from '../../src/helpers/json.js';
import { inParallel } from '../../src/helpers/parallel.js';
import { SeededRandom } from '../../src/helpers/random.js';
import {
  expectedTier,
  ledgerProblems,
  outboxProblems,
  qualifyingPointsOf,
} from '../../src/helpers/reconciliation.js';

/**
 * Ledger reconciliation: whatever sequence of writes a customer goes through,
 * including rejected ones and retries, the ledger stays consistent with
 * itself, with the balance and tier the API reports, and with the events the
 * engine published. Each random run uses a fixed seed, so a failure replays
 * exactly.
 */

const SEEDS = [101, 202, 303];
const OPERATIONS_PER_RUN = 30;

interface RunResult {
  /** What the test expects, built only from the API's answers. */
  model: { balance: number; accepted: number; rejected: number };
  log: string[];
}

/**
 * A random mix of earns, redeems (sometimes more than the balance), refunds
 * (sometimes more than is refundable) and idempotent retries.
 */
async function randomRun(loyalty: LoyaltyClient, customerId: string, random: SeededRandom): Promise<RunResult> {
  const model = { balance: 0, accepted: 0, rejected: 0 };
  const earns: { transactionId: string; refundable: number }[] = [];
  const log: string[] = [];

  for (let step = 0; step < OPERATIONS_PER_RUN; step += 1) {
    const kind = random.pick(['earn', 'earn', 'redeem', 'refund', 'retry'] as const);
    let response;
    if (kind === 'earn' || (kind === 'refund' && earns.length === 0)) {
      response = await loyalty.earn(customerId, anEarn().points(random.int(1, 2500)).build());
    } else if (kind === 'redeem') {
      // Up to 20% more than the balance, so some redeems are rejected.
      response = await loyalty.redeem(customerId, aRedeem().points(random.int(1, Math.ceil(model.balance * 1.2) + 1)).build());
    } else if (kind === 'refund') {
      const earn = random.pick(earns);
      response = await loyalty.refund(customerId, aRefundOf(earn.transactionId).points(random.int(1, earn.refundable + 50)).build());
    } else {
      // The same request twice with one key: the second is a replay, not a new entry.
      const key = uniqueIdempotencyKey();
      const body = anEarn().points(random.int(1, 500)).build();
      await expect(await loyalty.earn(customerId, body, { idempotencyKey: key })).toHaveStatus(201);
      response = await loyalty.earn(customerId, body, { idempotencyKey: key });
    }

    const status = response.status();
    if (status === 201) {
      // For a retry this is the replayed answer of the first call, so it is counted once.
      const result = await readJson<TransactionResponse>(response);
      const { transaction } = result;
      model.balance += transaction.points;
      model.accepted += 1;
      if (transaction.type === 'EARN') earns.push({ transactionId: transaction.transactionId, refundable: transaction.points });
      if (transaction.type === 'REFUND') {
        const earn = earns.find((e) => e.transactionId === transaction.refundOf)!;
        earn.refundable += transaction.points;
      }
      log.push(`${step}: ${kind} ${transaction.points} -> balance ${result.balance}`);
    } else {
      expect([409, 422], `step ${step} (${kind}): only business rule rejections are expected`).toContain(status);
      model.rejected += 1;
      log.push(`${step}: ${kind} rejected with ${status}`);
    }
  }
  return { model, log };
}

async function snapshot(loyalty: LoyaltyClient, customerId: string) {
  const [ledger, customer, outbox] = await Promise.all([
    loyalty.getLedger(customerId).then((r) => readJson<LedgerResponse>(r)),
    loyalty.getCustomer(customerId).then((r) => readJson<CustomerResponse>(r)),
    loyalty.getOutbox(customerId).then((r) => readJson<OutboxResponse>(r)),
  ]);
  return { ledger, customer, outbox };
}

test.describe('Ledger reconciliation', () => {
  for (const seed of SEEDS) {
    test(`random run (seed ${seed}): ledger, balance, tier and events reconcile`, async ({ loyalty, customer }, testInfo) => {
      const { model, log } = await randomRun(loyalty, customer.customerId, new SeededRandom(seed));
      await testInfo.attach('operations', { body: log.join('\n'), contentType: 'text/plain' });
      expect(model.rejected, 'the run should include rejected writes').toBeGreaterThan(0);

      const { ledger, customer: summary, outbox } = await snapshot(loyalty, customer.customerId);

      expect(ledgerProblems(ledger), 'ledger rules').toEqual([]);
      expect(ledger.entries, 'one entry per accepted write; rejections and replays add none').toHaveLength(model.accepted);
      expect(ledger.balance, 'ledger balance vs the API answers').toBe(model.balance);
      expect(summary.balance, 'customer balance vs ledger').toBe(ledger.balance);
      expect(summary.qualifyingPoints, 'qualifying points vs ledger').toBe(qualifyingPointsOf(ledger.entries));
      expect(summary.tier, 'tier vs ledger').toBe(expectedTier(qualifyingPointsOf(ledger.entries)));
      expect(outboxProblems(ledger.entries, outbox.events), 'outbox vs ledger').toEqual([]);
    });
  }

  test('parallel earns and redeems for one customer reconcile and never overdraw', async ({ loyalty, customer, earnPoints }) => {
    await earnPoints(customer.customerId, 1000);
    // 10 redeems of 150 compete for 1000 points while 10 earns of 20 add more.
    const responses = await inParallel(loyalty, 20, (i) =>
      i % 2 === 0
        ? loyalty.redeem(customer.customerId, aRedeem().points(150).build())
        : loyalty.earn(customer.customerId, anEarn().points(20).build()),
    );
    const statuses = responses.map((r) => r.status());
    expect(statuses.every((s) => s === 201 || s === 422), statuses.join()).toBe(true);

    const { ledger, customer: summary, outbox } = await snapshot(loyalty, customer.customerId);
    expect(ledgerProblems(ledger)).toEqual([]);
    expect(ledger.entries).toHaveLength(1 + statuses.filter((s) => s === 201).length);
    expect(summary.balance).toBe(ledger.balance);
    expect(outboxProblems(ledger.entries, outbox.events)).toEqual([]);
  });

  test('the ledger is append-only: earlier entries never change', async ({ loyalty, customer, earnPoints }) => {
    const first = await earnPoints(customer.customerId, 1200);
    await expect(await loyalty.redeem(customer.customerId, aRedeem().points(200).build())).toHaveStatus(201);
    const before = (await snapshot(loyalty, customer.customerId)).ledger.entries;

    // A refund, a downgrade and a rejected write later...
    await expect(
      await loyalty.refund(customer.customerId, aRefundOf(first.transaction.transactionId).points(500).build()),
    ).toHaveStatus(201);
    await expect(await loyalty.redeem(customer.customerId, aRedeem().points(99_999).build())).toHaveStatus(422);
    await earnPoints(customer.customerId, 10);
    const after = (await snapshot(loyalty, customer.customerId)).ledger.entries;

    expect(after.slice(0, before.length), 'existing entries are unchanged').toEqual(before);
    expect(after.slice(before.length).map((e: LedgerEntry) => e.type)).toEqual(['REFUND', 'EARN']);
  });

  test('ledgers of different customers stay separate under parallel load', async ({ loyalty, createCustomer }) => {
    const customers = await Promise.all(Array.from({ length: 5 }, () => createCustomer()));
    await inParallel(loyalty, 25, (i) => loyalty.earn(customers[i % 5]!.customerId, anEarn().points(i + 1).build()));

    for (const [index, { customerId }] of customers.entries()) {
      const { ledger } = await snapshot(loyalty, customerId);
      // Customer k got requests k, k+5, ..., k+20, with points index+1.
      const expected = [0, 1, 2, 3, 4].reduce((sum, round) => sum + index + round * 5 + 1, 0);
      expect(ledgerProblems(ledger), customerId).toEqual([]);
      expect(ledger.entries, customerId).toHaveLength(5);
      expect(ledger.balance, customerId).toBe(expected);
    }
  });
});

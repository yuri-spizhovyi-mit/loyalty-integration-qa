import { uniqueCustomerId } from '../../src/builders/customer-builder.js';
import { anEarn } from '../../src/builders/transaction-builder.js';
import type { ErrorResponse, LedgerResponse, TransactionResponse } from '../../src/clients/loyalty-types.js';
import { expect, test } from '../../src/fixtures.js';
import { ISO_8601_UTC, readJson } from '../../src/helpers/json.js';

const MAX_POINTS = 1_000_000;

test.describe('Earn points', () => {
  test('earning adds points and writes one EARN ledger entry', async ({ loyalty, customer, correlationId }) => {
    const request = anEarn().points(250).reference('order-1001').build();

    const response = await loyalty.earn(customer.customerId, request);

    await expect(response).toHaveStatus(201);
    const body = await readJson<TransactionResponse>(response);
    expect(body).toMatchObject({ balance: 250, qualifyingPoints: 250, tier: 'Bronze' });
    expect(body.transaction).toMatchObject({
      customerId: customer.customerId,
      type: 'EARN',
      points: 250,
      balanceAfter: 250,
      reference: 'order-1001',
      refundOf: null,
      correlationId,
    });
    expect(body.transaction.transactionId).toMatch(/^txn_[0-9a-f-]{36}$/);
    expect(body.transaction.createdAt).toMatch(ISO_8601_UTC);

    const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(customer.customerId));
    expect(ledger.entries).toEqual([body.transaction]);
    expect(ledger.balance).toBe(250);
  });

  test('several earns add up and each entry records the running balance', async ({ loyalty, customer, earnPoints }) => {
    await earnPoints(customer.customerId, 100);
    await earnPoints(customer.customerId, 40);
    await earnPoints(customer.customerId, 7);

    const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(customer.customerId));
    expect(ledger.balance).toBe(147);
    expect(ledger.entries.map((entry) => [entry.points, entry.balanceAfter])).toEqual([
      [100, 100],
      [40, 140],
      [7, 147],
    ]);
  });

  test('reference is optional and stored as null when omitted', async ({ loyalty, customer }) => {
    const response = await loyalty.earn(customer.customerId, anEarn().withoutReference().build());

    await expect(response).toHaveStatus(201);
    expect((await readJson<TransactionResponse>(response)).transaction.reference).toBeNull();
  });

  test.describe('points boundaries', () => {
    for (const points of [1, MAX_POINTS]) {
      test(`${points} points is accepted`, async ({ loyalty, customer }) => {
        const response = await loyalty.earn(customer.customerId, anEarn().points(points).build());

        await expect(response).toHaveStatus(201);
        expect((await readJson<TransactionResponse>(response)).balance).toBe(points);
      });
    }

    test(`${MAX_POINTS + 1} points (max + 1) is rejected`, async ({ loyalty, customer }) => {
      const response = await loyalty.earn(customer.customerId, anEarn().points(MAX_POINTS + 1).build());

      await expect(response).toBeApiError(400, 'VALIDATION_ERROR');
    });
  });

  const invalidPoints: { name: string; points: unknown }[] = [
    { name: 'zero', points: 0 },
    { name: 'negative', points: -1 },
    { name: 'a decimal', points: 10.5 },
    { name: 'a numeric string', points: '100' },
    { name: 'null', points: null },
    { name: 'a boolean', points: true },
  ];
  for (const { name, points } of invalidPoints) {
    test(`points that is ${name} is rejected and nothing is written`, async ({ loyalty, customer }) => {
      const response = await loyalty.earn(customer.customerId, { points });

      await expect(response).toBeApiError(400, 'VALIDATION_ERROR');
      expect((await readJson<ErrorResponse>(response)).error.details).toContainEqual(
        expect.objectContaining({ field: 'points' }),
      );
      const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(customer.customerId));
      expect(ledger.entries).toHaveLength(0);
    });
  }

  test('missing points is rejected with 400', async ({ loyalty, customer }) => {
    const response = await loyalty.earn(customer.customerId, { reference: 'order-without-points' });

    await expect(response).toBeApiError(400, 'VALIDATION_ERROR');
  });

  test('reference length boundary: 128 accepted, 129 rejected', async ({ loyalty, customer }) => {
    const ok = await loyalty.earn(customer.customerId, anEarn().reference('r'.repeat(128)).build());
    const tooLong = await loyalty.earn(customer.customerId, anEarn().reference('r'.repeat(129)).build());

    await expect(ok).toHaveStatus(201);
    await expect(tooLong).toBeApiError(400, 'VALIDATION_ERROR');
  });

  test('a client cannot set server-owned fields such as balance', async ({ loyalty, customer }) => {
    const response = await loyalty.earn(customer.customerId, { points: 10, balance: 1_000_000 });

    await expect(response).toBeApiError(400, 'VALIDATION_ERROR');
  });

  test('earning for an unknown customer returns 404', async ({ loyalty }) => {
    const response = await loyalty.earn(uniqueCustomerId('missing'), anEarn().build());

    await expect(response).toBeApiError(404, 'CUSTOMER_NOT_FOUND');
  });
});

import { uniqueCustomerId } from '../../src/builders/customer-builder.js';
import { aRedeem } from '../../src/builders/transaction-builder.js';
import type { LedgerResponse, TransactionResponse } from '../../src/clients/loyalty-types.js';
import { expect, test } from '../../src/fixtures.js';
import { readJson } from '../../src/helpers/json.js';
import { inParallel } from '../../src/helpers/parallel.js';

test.describe('Redeem points', () => {
  test('redeeming deducts points and writes a negative REDEEM entry', async ({ loyalty, customer, earnPoints }) => {
    await earnPoints(customer.customerId, 500);

    const response = await loyalty.redeem(customer.customerId, aRedeem().points(120).build());

    await expect(response).toHaveStatus(201);
    const body = await readJson<TransactionResponse>(response);
    expect(body.balance).toBe(380);
    expect(body.transaction).toMatchObject({ type: 'REDEEM', points: -120, balanceAfter: 380, refundOf: null });
  });

  test('redeeming the exact balance leaves zero', async ({ loyalty, customer, earnPoints }) => {
    await earnPoints(customer.customerId, 300);

    const response = await loyalty.redeem(customer.customerId, aRedeem().points(300).build());

    await expect(response).toHaveStatus(201);
    expect((await readJson<TransactionResponse>(response)).balance).toBe(0);
  });

  test('redeeming balance + 1 is rejected and the ledger is unchanged', async ({ loyalty, customer, earnPoints }) => {
    await earnPoints(customer.customerId, 300);

    const response = await loyalty.redeem(customer.customerId, aRedeem().points(301).build());

    await expect(response).toBeApiError(422, 'INSUFFICIENT_BALANCE');
    const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(customer.customerId));
    expect(ledger.balance).toBe(300);
    expect(ledger.entries).toHaveLength(1);
  });

  test('redeeming with a zero balance is rejected', async ({ loyalty, customer }) => {
    const response = await loyalty.redeem(customer.customerId, aRedeem().points(1).build());

    await expect(response).toBeApiError(422, 'INSUFFICIENT_BALANCE');
  });

  for (const points of [0, -5]) {
    test(`redeeming ${points} points is a validation error, not a balance error`, async ({ loyalty, customer }) => {
      const response = await loyalty.redeem(customer.customerId, { points });

      await expect(response).toBeApiError(400, 'VALIDATION_ERROR');
    });
  }

  test('redeeming does not lower the tier', async ({ loyalty, customer, earnPoints }) => {
    await earnPoints(customer.customerId, 1200);

    const response = await loyalty.redeem(customer.customerId, aRedeem().points(1100).build());

    await expect(response).toHaveStatus(201);
    const body = await readJson<TransactionResponse>(response);
    expect(body).toMatchObject({ balance: 100, qualifyingPoints: 1200, tier: 'Silver' });
  });

  test('parallel redeems with different keys never overdraw the balance', async ({ loyalty, customer, earnPoints }) => {
    // 100 points, five parallel requests for 30 each: exactly three can succeed.
    await earnPoints(customer.customerId, 100);

    const responses = await inParallel(loyalty, 5, () =>
      loyalty.redeem(customer.customerId, aRedeem().points(30).build()),
    );

    const statuses = responses.map((response) => response.status()).sort();
    expect(statuses).toEqual([201, 201, 201, 422, 422]);
    const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(customer.customerId));
    expect(ledger.balance).toBe(10);
    expect(ledger.entries.filter((entry) => entry.type === 'REDEEM')).toHaveLength(3);
    expect(ledger.entries.every((entry) => entry.balanceAfter >= 0)).toBe(true);
  });

  test('redeeming for an unknown customer returns 404', async ({ loyalty }) => {
    const response = await loyalty.redeem(uniqueCustomerId('missing'), aRedeem().build());

    await expect(response).toBeApiError(404, 'CUSTOMER_NOT_FOUND');
  });
});

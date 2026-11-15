import { aRedeem, aRefundOf } from '../../src/builders/transaction-builder.js';
import type { LedgerResponse, TransactionResponse } from '../../src/clients/loyalty-types.js';
import { expect, test } from '../../src/fixtures.js';
import { readJson } from '../../src/helpers/json.js';

test.describe('Refund points', () => {
  test('a refund without points reverses the whole earn', async ({ loyalty, customer, earnPoints }) => {
    const earn = await earnPoints(customer.customerId, 400);

    const response = await loyalty.refund(customer.customerId, aRefundOf(earn.transaction.transactionId).build());

    await expect(response).toHaveStatus(201);
    const body = await readJson<TransactionResponse>(response);
    expect(body).toMatchObject({ balance: 0, qualifyingPoints: 0 });
    expect(body.transaction).toMatchObject({
      type: 'REFUND',
      points: -400,
      refundOf: earn.transaction.transactionId,
    });
  });

  test('partial refunds can add up to the original, but not past it', async ({ loyalty, customer, earnPoints }) => {
    const earn = await earnPoints(customer.customerId, 100);
    const txId = earn.transaction.transactionId;

    await expect(await loyalty.refund(customer.customerId, aRefundOf(txId).points(60).build())).toHaveStatus(201);
    await expect(await loyalty.refund(customer.customerId, aRefundOf(txId).points(40).build())).toHaveStatus(201);
    const third = await loyalty.refund(customer.customerId, aRefundOf(txId).points(1).build());

    await expect(third).toBeApiError(422, 'REFUND_EXCEEDS_ORIGINAL');
    const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(customer.customerId));
    expect(ledger.balance).toBe(0);
  });

  test('a full refund after a partial one refunds only what is left', async ({ loyalty, customer, earnPoints }) => {
    const earn = await earnPoints(customer.customerId, 100);
    const txId = earn.transaction.transactionId;
    await loyalty.refund(customer.customerId, aRefundOf(txId).points(30).build());

    const response = await loyalty.refund(customer.customerId, aRefundOf(txId).build());

    await expect(response).toHaveStatus(201);
    expect((await readJson<TransactionResponse>(response)).transaction.points).toBe(-70);
  });

  test('refunding more than the original earn is rejected', async ({ loyalty, customer, earnPoints }) => {
    await earnPoints(customer.customerId, 500); // extra balance, so the check is the refund rule
    const earn = await earnPoints(customer.customerId, 100);

    const response = await loyalty.refund(
      customer.customerId,
      aRefundOf(earn.transaction.transactionId).points(101).build(),
    );

    await expect(response).toBeApiError(422, 'REFUND_EXCEEDS_ORIGINAL');
  });

  test('refunding an unknown transaction returns 404', async ({ loyalty, customer, earnPoints }) => {
    await earnPoints(customer.customerId, 100);

    const response = await loyalty.refund(customer.customerId, aRefundOf('txn_does-not-exist').build());

    await expect(response).toBeApiError(404, 'TRANSACTION_NOT_FOUND');
  });

  test("a customer cannot refund another customer's transaction", async ({
    loyalty,
    createCustomer,
    earnPoints,
  }) => {
    const owner = await createCustomer();
    const other = await createCustomer();
    const earn = await earnPoints(owner.customerId, 100);
    await earnPoints(other.customerId, 100);

    const response = await loyalty.refund(other.customerId, aRefundOf(earn.transaction.transactionId).build());

    await expect(response).toBeApiError(404, 'TRANSACTION_NOT_FOUND');
    const ownerLedger = await readJson<LedgerResponse>(await loyalty.getLedger(owner.customerId));
    expect(ownerLedger.balance).toBe(100);
  });

  test('a REDEEM entry cannot be refunded', async ({ loyalty, customer, earnPoints }) => {
    await earnPoints(customer.customerId, 100);
    const redeem = await readJson<TransactionResponse>(
      await loyalty.redeem(customer.customerId, aRedeem().points(50).build()),
    );

    const response = await loyalty.refund(customer.customerId, aRefundOf(redeem.transaction.transactionId).build());

    await expect(response).toBeApiError(422, 'REFUND_NOT_ALLOWED');
  });

  test('a refund is rejected when the points were already redeemed', async ({ loyalty, customer, earnPoints }) => {
    const earn = await earnPoints(customer.customerId, 100);
    await expect(await loyalty.redeem(customer.customerId, aRedeem().points(80).build())).toHaveStatus(201);

    const response = await loyalty.refund(customer.customerId, aRefundOf(earn.transaction.transactionId).build());

    await expect(response).toBeApiError(422, 'INSUFFICIENT_BALANCE');
    const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(customer.customerId));
    expect(ledger.balance).toBe(20);
  });

  test('a refund can move the customer down a tier', async ({ loyalty, customer, earnPoints }) => {
    await earnPoints(customer.customerId, 900);
    const purchase = await earnPoints(customer.customerId, 200);
    expect(purchase.tier).toBe('Silver');

    const response = await loyalty.refund(customer.customerId, aRefundOf(purchase.transaction.transactionId).build());

    await expect(response).toHaveStatus(201);
    expect(await readJson<TransactionResponse>(response)).toMatchObject({ qualifyingPoints: 900, tier: 'Bronze' });
  });

  test('missing transactionId is rejected with 400', async ({ loyalty, customer }) => {
    const response = await loyalty.refund(customer.customerId, { points: 10 });

    await expect(response).toBeApiError(400, 'VALIDATION_ERROR');
  });
});

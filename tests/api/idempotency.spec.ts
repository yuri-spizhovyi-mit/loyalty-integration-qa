import { anEarn, aRedeem, uniqueIdempotencyKey } from '../../src/builders/transaction-builder.js';
import type { LedgerResponse, TransactionResponse } from '../../src/clients/loyalty-types.js';
import { expect, test } from '../../src/fixtures.js';
import { readJson } from '../../src/helpers/json.js';
import { inParallel } from '../../src/helpers/parallel.js';

test.describe('Idempotency-Key', () => {
  test('the same request sent twice with one key writes one ledger entry', async ({ loyalty, customer }) => {
    const key = uniqueIdempotencyKey();
    const request = anEarn().points(100).build();

    const first = await loyalty.earn(customer.customerId, request, { idempotencyKey: key });
    const second = await loyalty.earn(customer.customerId, request, { idempotencyKey: key });

    await expect(first).toHaveStatus(201);
    await expect(second).toHaveStatus(201);
    expect(first.headers()['idempotent-replayed']).toBe('false');
    expect(second.headers()['idempotent-replayed']).toBe('true');
    expect(await second.json()).toEqual(await first.json());

    const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(customer.customerId));
    expect(ledger.entries).toHaveLength(1);
    expect(ledger.balance).toBe(100);
  });

  test('without a key, the same request sent twice writes two entries', async ({ loyalty, customer }) => {
    // The baseline that shows why clients need the key on retries.
    const request = anEarn().points(100).build();

    await loyalty.earn(customer.customerId, request);
    await loyalty.earn(customer.customerId, request);

    const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(customer.customerId));
    expect(ledger.entries).toHaveLength(2);
  });

  test('ten parallel requests with one key produce one entry and identical responses', async ({
    loyalty,
    customer,
  }) => {
    const key = uniqueIdempotencyKey();
    const request = anEarn().points(100).build();

    const responses = await inParallel(loyalty, 10, () =>
      loyalty.earn(customer.customerId, request, { idempotencyKey: key }),
    );

    for (const response of responses) await expect(response).toHaveStatus(201);
    const bodies = await Promise.all(responses.map((response) => readJson<TransactionResponse>(response)));
    expect(new Set(bodies.map((body) => body.transaction.transactionId)).size).toBe(1);
    expect(responses.filter((response) => response.headers()['idempotent-replayed'] === 'false')).toHaveLength(1);

    const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(customer.customerId));
    expect(ledger.entries).toHaveLength(1);
    expect(ledger.balance).toBe(100);
  });

  test('reusing a key with a different body is rejected with 409', async ({ loyalty, customer }) => {
    const key = uniqueIdempotencyKey();
    await expect(
      await loyalty.earn(customer.customerId, anEarn().points(100).build(), { idempotencyKey: key }),
    ).toHaveStatus(201);

    const reused = await loyalty.earn(customer.customerId, anEarn().points(999).build(), { idempotencyKey: key });

    await expect(reused).toBeApiError(409, 'IDEMPOTENCY_KEY_REUSED');
    const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(customer.customerId));
    expect(ledger.balance).toBe(100);
  });

  test('reusing a key on a different endpoint is rejected with 409', async ({ loyalty, customer, earnPoints }) => {
    await earnPoints(customer.customerId, 500);
    const key = uniqueIdempotencyKey();
    const body = { points: 50, reference: 'same-body' };
    await expect(await loyalty.earn(customer.customerId, body, { idempotencyKey: key })).toHaveStatus(201);

    const reused = await loyalty.redeem(customer.customerId, body, { idempotencyKey: key });

    await expect(reused).toBeApiError(409, 'IDEMPOTENCY_KEY_REUSED');
  });

  test('reusing a key for a different customer is rejected with 409', async ({ loyalty, createCustomer }) => {
    const first = await createCustomer();
    const second = await createCustomer();
    const key = uniqueIdempotencyKey();
    const request = anEarn().points(100).build();
    await expect(await loyalty.earn(first.customerId, request, { idempotencyKey: key })).toHaveStatus(201);

    const reused = await loyalty.earn(second.customerId, request, { idempotencyKey: key });

    await expect(reused).toBeApiError(409, 'IDEMPOTENCY_KEY_REUSED');
    const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(second.customerId));
    expect(ledger.entries).toHaveLength(0);
  });

  test('different keys are different transactions', async ({ loyalty, customer }) => {
    const request = anEarn().points(100).build();

    await loyalty.earn(customer.customerId, request, { idempotencyKey: uniqueIdempotencyKey() });
    await loyalty.earn(customer.customerId, request, { idempotencyKey: uniqueIdempotencyKey() });

    const ledger = await readJson<LedgerResponse>(await loyalty.getLedger(customer.customerId));
    expect(ledger.entries).toHaveLength(2);
  });

  test('a failed request is not stored, so a retry with the same key can succeed', async ({
    loyalty,
    customer,
    earnPoints,
  }) => {
    const key = uniqueIdempotencyKey();
    const request = aRedeem().points(100).build();
    await expect(await loyalty.redeem(customer.customerId, request, { idempotencyKey: key })).toBeApiError(
      422,
      'INSUFFICIENT_BALANCE',
    );

    await earnPoints(customer.customerId, 100);
    const retry = await loyalty.redeem(customer.customerId, request, { idempotencyKey: key });

    await expect(retry).toHaveStatus(201);
    expect(retry.headers()['idempotent-replayed']).toBe('false');
    expect((await readJson<TransactionResponse>(retry)).balance).toBe(0);
  });

  test('key length boundary: 255 characters accepted, 256 rejected', async ({ loyalty, customer }) => {
    // Keys are global, so pad a unique key rather than using a fixed string.
    const ok = await loyalty.earn(customer.customerId, anEarn().build(), {
      idempotencyKey: uniqueIdempotencyKey().padEnd(255, 'k'),
    });
    const tooLong = await loyalty.earn(customer.customerId, anEarn().build(), {
      idempotencyKey: uniqueIdempotencyKey().padEnd(256, 'k'),
    });

    await expect(ok).toHaveStatus(201);
    await expect(tooLong).toBeApiError(400, 'VALIDATION_ERROR');
  });
});

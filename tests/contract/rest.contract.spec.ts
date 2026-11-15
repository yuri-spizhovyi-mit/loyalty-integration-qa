import type { APIResponse } from '@playwright/test';
import { uniqueCustomerId } from '../../src/builders/customer-builder.js';
import { anEarn, aRedeem, aRefundOf, uniqueIdempotencyKey } from '../../src/builders/transaction-builder.js';
import type { LoyaltyClient } from '../../src/clients/loyalty-client.js';
import type { CreateCustomerBody, TransactionResponse } from '../../src/clients/loyalty-types.js';
import { expect, test } from '../../src/fixtures.js';
import { getContract } from '../../src/helpers/schema-validator.js';

/**
 * One scenario per documented (operationId, status) pair in openapi.yaml.
 * The last test fails if the spec documents a response no scenario covers,
 * so a new endpoint or status code cannot be added without a contract test.
 */

interface Ctx {
  loyalty: LoyaltyClient;
  createCustomer: (body?: CreateCustomerBody) => Promise<CreateCustomerBody>;
  earnPoints: (customerId: string, points: number) => Promise<TransactionResponse>;
}

type Scenario = (ctx: Ctx) => Promise<APIResponse>;

const missing = () => uniqueCustomerId('missing');

const scenarios: Record<string, Scenario> = {
  'getHealth 200': ({ loyalty }) => loyalty.health(),

  'createCustomer 201': ({ loyalty }) => loyalty.createCustomer({ customerId: uniqueCustomerId(), marketingConsent: true }),
  'createCustomer 400': ({ loyalty }) => loyalty.createCustomer({ customerId: 'x' }),
  'createCustomer 409': async ({ loyalty, createCustomer }) => loyalty.createCustomer(await createCustomer()),

  'getCustomer 200': async ({ loyalty, createCustomer }) => loyalty.getCustomer((await createCustomer()).customerId),
  'getCustomer 404': ({ loyalty }) => loyalty.getCustomer(missing()),

  'getTier 200': async ({ loyalty, createCustomer, earnPoints }) => {
    const { customerId } = await createCustomer();
    await earnPoints(customerId, 5000); // Gold: the null nextTier branch
    return loyalty.getTier(customerId);
  },
  'getTier 404': ({ loyalty }) => loyalty.getTier(missing()),

  'getLedger 200': async ({ loyalty, createCustomer, earnPoints }) => {
    // All three entry types, so each branch of the LedgerEntry schema is used.
    const { customerId } = await createCustomer();
    const earn = await earnPoints(customerId, 300);
    await loyalty.redeem(customerId, aRedeem().points(50).build());
    await loyalty.refund(customerId, aRefundOf(earn.transaction.transactionId).points(20).build());
    return loyalty.getLedger(customerId);
  },
  'getLedger 404': ({ loyalty }) => loyalty.getLedger(missing()),

  'earnPoints 201': async ({ loyalty, createCustomer }) =>
    loyalty.earn((await createCustomer()).customerId, anEarn().build()),
  'earnPoints 400': async ({ loyalty, createCustomer }) =>
    loyalty.earn((await createCustomer()).customerId, { points: 0 }),
  'earnPoints 404': ({ loyalty }) => loyalty.earn(missing(), anEarn().build()),
  'earnPoints 409': async ({ loyalty, createCustomer }) => {
    const { customerId } = await createCustomer();
    const key = uniqueIdempotencyKey();
    await loyalty.earn(customerId, anEarn().points(1).build(), { idempotencyKey: key });
    return loyalty.earn(customerId, anEarn().points(2).build(), { idempotencyKey: key });
  },

  'redeemPoints 201': async ({ loyalty, createCustomer, earnPoints }) => {
    const { customerId } = await createCustomer();
    await earnPoints(customerId, 100);
    return loyalty.redeem(customerId, aRedeem().points(40).build());
  },
  'redeemPoints 400': async ({ loyalty, createCustomer }) =>
    loyalty.redeem((await createCustomer()).customerId, { points: -1 }),
  'redeemPoints 404': ({ loyalty }) => loyalty.redeem(missing(), aRedeem().build()),
  'redeemPoints 409': async ({ loyalty, createCustomer, earnPoints }) => {
    const { customerId } = await createCustomer();
    await earnPoints(customerId, 100);
    const key = uniqueIdempotencyKey();
    await loyalty.redeem(customerId, aRedeem().points(1).build(), { idempotencyKey: key });
    return loyalty.redeem(customerId, aRedeem().points(2).build(), { idempotencyKey: key });
  },
  'redeemPoints 422': async ({ loyalty, createCustomer }) =>
    loyalty.redeem((await createCustomer()).customerId, aRedeem().points(1).build()),

  'refundPoints 201': async ({ loyalty, createCustomer, earnPoints }) => {
    const { customerId } = await createCustomer();
    const earn = await earnPoints(customerId, 100);
    return loyalty.refund(customerId, aRefundOf(earn.transaction.transactionId).build());
  },
  'refundPoints 400': async ({ loyalty, createCustomer }) =>
    loyalty.refund((await createCustomer()).customerId, { points: 5 }),
  'refundPoints 404': async ({ loyalty, createCustomer }) =>
    loyalty.refund((await createCustomer()).customerId, aRefundOf('txn_unknown').build()),
  'refundPoints 409': async ({ loyalty, createCustomer, earnPoints }) => {
    const { customerId } = await createCustomer();
    const earn = await earnPoints(customerId, 100);
    const key = uniqueIdempotencyKey();
    await loyalty.refund(customerId, aRefundOf(earn.transaction.transactionId).points(1).build(), { idempotencyKey: key });
    return loyalty.refund(customerId, aRefundOf(earn.transaction.transactionId).points(2).build(), {
      idempotencyKey: key,
    });
  },
  'refundPoints 422': async ({ loyalty, createCustomer, earnPoints }) => {
    const { customerId } = await createCustomer();
    const earn = await earnPoints(customerId, 100);
    return loyalty.refund(customerId, aRefundOf(earn.transaction.transactionId).points(101).build());
  },

  'listOutboxEvents 200': async ({ loyalty, createCustomer, earnPoints }) => {
    const { customerId } = await createCustomer();
    await earnPoints(customerId, 1000);
    return loyalty.getOutbox(customerId);
  },
  'listOutboxEvents 400': ({ loyalty }) => loyalty.getOutbox('x'),
};

test.describe('REST responses match openapi.yaml', () => {
  for (const [key, scenario] of Object.entries(scenarios)) {
    const [operationId, status] = key.split(' ') as [string, string];
    test(key, async ({ loyalty, createCustomer, earnPoints }) => {
      const response = await scenario({ loyalty, createCustomer, earnPoints });

      await expect(response).toHaveStatus(Number(status));
      await expect(response).toMatchContract(operationId);
    });
  }

  test('every documented response has a scenario above', () => {
    const documented = getContract()
      .operations()
      .flatMap(({ operationId, statuses }) => statuses.map((status) => `${operationId} ${status}`))
      .sort();

    expect(Object.keys(scenarios).sort()).toEqual(documented);
  });
});

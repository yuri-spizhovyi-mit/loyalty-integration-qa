import { uniqueCustomerId } from '../../src/builders/customer-builder.js';
import { expect, test } from '../../src/fixtures.js';
import { getContract } from '../../src/helpers/schema-validator.js';

/**
 * The service validates requests with its own code, not with the spec. These
 * tests send the same bodies to both and check they agree: the server returns
 * 400 exactly when the OpenAPI request schema says the body is invalid.
 * A disagreement means the docs and the service have drifted apart, and a
 * client built from the spec will be surprised.
 */
const contract = getContract();

function cases(...bodies: [string, unknown][]) {
  return bodies.map(([name, body]) => ({ name, body }));
}

const earnBodies = cases(
  ['minimum points', { points: 1 }],
  ['maximum points', { points: 1_000_000 }],
  ['with a reference', { points: 5, reference: 'order-9' }],
  ['reference at 128 chars', { points: 5, reference: 'r'.repeat(128) }],
  ['zero points', { points: 0 }],
  ['negative points', { points: -3 }],
  ['points above maximum', { points: 1_000_001 }],
  ['decimal points', { points: 2.5 }],
  ['points as a string', { points: '5' }],
  ['missing points', {}],
  ['empty reference', { points: 5, reference: '' }],
  ['reference at 129 chars', { points: 5, reference: 'r'.repeat(129) }],
  ['null reference', { points: 5, reference: null }],
  ['unknown field', { points: 5, channel: 'web' }],
);

const refundBodies = cases(
  ['transactionId only', { transactionId: 'txn_unknown' }],
  ['transactionId and points', { transactionId: 'txn_unknown', points: 5 }],
  ['missing transactionId', { points: 5 }],
  ['empty transactionId', { transactionId: '' }],
  ['numeric transactionId', { transactionId: 42 }],
  ['zero points', { transactionId: 'txn_unknown', points: 0 }],
  ['unknown field', { transactionId: 'txn_unknown', reason: 'damaged' }],
);

const customerBodies = cases(
  ['id only', { customerId: '__ID__' }],
  ['id and consent', { customerId: '__ID__', marketingConsent: true }],
  ['id too short', { customerId: 'ab' }],
  ['id with a dot', { customerId: 'qa.customer' }],
  ['consent as a string', { customerId: '__ID__', marketingConsent: 'true' }],
  ['missing id', { marketingConsent: false }],
  ['unknown field', { customerId: '__ID__', tier: 'Gold' }],
);

test.describe('Request validation: server agrees with the spec', () => {
  for (const { name, body } of earnBodies) {
    test(`earnPoints: ${name}`, async ({ loyalty, customer }) => {
      const specValid = contract.validateRequest('earnPoints', body).valid;

      const response = await loyalty.earn(customer.customerId, body as Record<string, unknown>);

      expect(response.status() === 400, `spec says ${specValid ? 'valid' : 'invalid'}; server returned ${response.status()}`).toBe(!specValid);
    });
  }

  for (const { name, body } of refundBodies) {
    test(`refundPoints: ${name}`, async ({ loyalty, customer }) => {
      const specValid = contract.validateRequest('refundPoints', body).valid;

      // A valid body still fails (404, unknown transaction), but not with 400.
      const response = await loyalty.refund(customer.customerId, body as Record<string, unknown>);

      expect(response.status() === 400, `spec says ${specValid ? 'valid' : 'invalid'}; server returned ${response.status()}`).toBe(!specValid);
    });
  }

  for (const { name, body } of customerBodies) {
    test(`createCustomer: ${name}`, async ({ loyalty }) => {
      const customerId = uniqueCustomerId();
      test.info().annotations.push({ type: 'customerId', description: customerId });
      const concrete = JSON.parse(JSON.stringify(body).replace('__ID__', customerId)) as Record<string, unknown>;
      const specValid = contract.validateRequest('createCustomer', concrete).valid;

      const response = await loyalty.createCustomer(concrete);

      expect(response.status() === 400, `spec says ${specValid ? 'valid' : 'invalid'}; server returned ${response.status()}`).toBe(!specValid);
    });
  }
});

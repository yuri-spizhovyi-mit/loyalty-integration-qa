import { expect, test } from '../../src/fixtures.js';
import { getContract } from '../../src/helpers/schema-validator.js';

/**
 * Checks on the specs and the validator themselves, with no service calls.
 * If these fail, every other schema assertion is meaningless.
 */
const contract = getContract();

const validCustomer = {
  customerId: 'qa-contract-1',
  marketingConsent: false,
  balance: 120,
  qualifyingPoints: 120,
  tier: 'Bronze',
  createdAt: '2026-11-14T10:15:00.000Z',
};

const validEarnEntry = {
  transactionId: 'txn_0b0d6f4e-8c1a-4d7e-9f3b-2a6c5e8d1f00',
  customerId: 'qa-contract-1',
  type: 'EARN',
  points: 120,
  balanceAfter: 120,
  reference: 'order-1',
  refundOf: null,
  correlationId: 'qa-corr-1',
  createdAt: '2026-11-14T10:15:00.000Z',
};

const validEarnedEvent = {
  eventId: '7d1f3c2a-5b6e-4f8a-9c0d-1e2f3a4b5c6d',
  type: 'points.earned',
  sequence: 1,
  customerId: 'qa-contract-1',
  correlationId: 'qa-corr-1',
  occurredAt: '2026-11-14T10:15:00.000Z',
  data: {
    transactionId: validEarnEntry.transactionId,
    entryType: 'EARN',
    points: 120,
    balanceAfter: 120,
    reference: 'order-1',
    refundOf: null,
  },
};

test.describe('Specs', () => {
  test('every operation has an operationId and at least one documented response', () => {
    for (const operation of contract.operations()) {
      expect(operation.operationId, `${operation.method} ${operation.path}`).not.toContain('missing operationId');
      expect(operation.statuses.length, operation.operationId).toBeGreaterThan(0);
    }
  });

  test('every documented response has a JSON schema that compiles', () => {
    for (const { operationId, statuses } of contract.operations()) {
      for (const status of statuses) {
        // Compiling happens on first use; a broken $ref or keyword throws here.
        const result = contract.validateResponse(operationId, status, {});
        expect(result.errors.join('; '), `${operationId} ${status}`).not.toMatch(/not documented|no application\/json/);
      }
    }
  });

  test('every AsyncAPI message has a payload schema that compiles', () => {
    for (const type of contract.eventTypes()) {
      const result = contract.validateEvent({ type });
      expect(result.errors.join('; '), type).not.toMatch(/not a message/);
    }
  });

  test('shared definitions are identical in openapi.yaml and asyncapi.yaml', () => {
    // The REST API and the events describe the same IDs and tiers. If the two
    // specs drift apart, a consumer can accept an event the API would reject.
    // `description` is documentation only, so it is left out of the comparison.
    const rules = (schema: unknown) => {
      const { description: _ignored, ...rest } = (schema ?? {}) as Record<string, unknown>;
      return rest;
    };
    for (const name of ['CustomerId', 'TransactionId', 'Tier', 'Timestamp']) {
      expect(rules(contract.asyncapi.components.schemas[name]), name).toEqual(
        rules(contract.openapi.components.schemas[name]),
      );
    }
  });
});

test.describe('Validator self-test: known-good payloads pass', () => {
  test('a valid customer, ledger entry and event pass', () => {
    expect(contract.validateResponse('getCustomer', 200, validCustomer).errors).toEqual([]);
    expect(contract.validateComponent('LedgerEntry', validEarnEntry).errors).toEqual([]);
    expect(contract.validateEvent(validEarnedEvent).errors).toEqual([]);
  });
});

test.describe('Validator self-test: known-bad payloads fail', () => {
  const badCustomers: { name: string; body: unknown; error: RegExp }[] = [
    { name: 'tier in lower case (mapping bug)', body: { ...validCustomer, tier: 'gold' }, error: /\/tier must be equal to one of/ },
    { name: 'balance as a string', body: { ...validCustomer, balance: '120' }, error: /\/balance must be integer/ },
    { name: 'balance as a decimal', body: { ...validCustomer, balance: 12.5 }, error: /\/balance must be integer/ },
    { name: 'negative balance', body: { ...validCustomer, balance: -1 }, error: /\/balance must be >= 0/ },
    { name: 'an extra field', body: { ...validCustomer, internalScore: 7 }, error: /additional properties "internalScore"/ },
    { name: 'createdAt missing', body: { ...validCustomer, createdAt: undefined }, error: /must have required property 'createdAt'/ },
    { name: 'createdAt not in UTC', body: { ...validCustomer, createdAt: '2026-11-14T10:15:00+02:00' }, error: /\/createdAt must match pattern/ },
  ];
  for (const { name, body, error } of badCustomers) {
    test(`customer with ${name}`, () => {
      const result = contract.validateResponse('getCustomer', 200, JSON.parse(JSON.stringify(body)));
      expect(result.valid).toBe(false);
      expect(result.errors.join('\n')).toMatch(error);
    });
  }

  const badEntries: { name: string; body: unknown }[] = [
    { name: 'EARN with negative points', body: { ...validEarnEntry, points: -120 } },
    { name: 'REDEEM with positive points', body: { ...validEarnEntry, type: 'REDEEM', points: 50 } },
    { name: 'REFUND without refundOf', body: { ...validEarnEntry, type: 'REFUND', points: -50, refundOf: null } },
    { name: 'EARN with a refundOf', body: { ...validEarnEntry, refundOf: validEarnEntry.transactionId } },
    { name: 'zero points', body: { ...validEarnEntry, points: 0 } },
    { name: 'a malformed transactionId', body: { ...validEarnEntry, transactionId: 'txn_123' } },
  ];
  for (const { name, body } of badEntries) {
    test(`ledger entry: ${name}`, () => {
      expect(contract.validateComponent('LedgerEntry', body).valid).toBe(false);
    });
  }

  test('tier status: Gold with a next tier is rejected', () => {
    const body = { customerId: 'qa-1', tier: 'Gold', qualifyingPoints: 6000, nextTier: 'Gold', pointsToNextTier: 1 };
    expect(contract.validateResponse('getTier', 200, body).valid).toBe(false);
  });

  test('tier status: Silver without a next tier is rejected', () => {
    const body = { customerId: 'qa-1', tier: 'Silver', qualifyingPoints: 1500, nextTier: null, pointsToNextTier: null };
    expect(contract.validateResponse('getTier', 200, body).valid).toBe(false);
  });

  test('error body with an unknown error code is rejected', () => {
    const body = { error: { code: 'SOMETHING_ELSE', message: 'x' }, correlationId: 'c' };
    expect(contract.validateResponse('earnPoints', 400, body).valid).toBe(false);
  });

  test('an undocumented status code is rejected even with a valid body', () => {
    const result = contract.validateResponse('getCustomer', 500, validCustomer);
    expect(result.errors).toEqual(['status 500 is not documented (documented: 200, 404)']);
  });

  const badEvents: { name: string; event: unknown }[] = [
    { name: 'an unknown type', event: { ...validEarnedEvent, type: 'points.expired' } },
    { name: 'type and data disagree', event: { ...validEarnedEvent, type: 'points.redeemed' } },
    { name: 'no eventId', event: { ...validEarnedEvent, eventId: undefined } },
    { name: 'eventId that is not a UUID', event: { ...validEarnedEvent, eventId: 'evt-1' } },
    { name: 'sequence 0', event: { ...validEarnedEvent, sequence: 0 } },
    { name: 'an extra envelope field', event: { ...validEarnedEvent, retryCount: 1 } },
    { name: 'an extra data field', event: { ...validEarnedEvent, data: { ...validEarnedEvent.data, tier: 'Gold' } } },
    { name: 'points as a string', event: { ...validEarnedEvent, data: { ...validEarnedEvent.data, points: '120' } } },
  ];
  for (const { name, event } of badEvents) {
    test(`event with ${name}`, () => {
      expect(contract.validateEvent(JSON.parse(JSON.stringify(event))).valid).toBe(false);
    });
  }

  test('a tier.changed event with an invalid direction is rejected', () => {
    const event = {
      ...validEarnedEvent,
      type: 'tier.changed',
      data: {
        previousTier: 'Bronze',
        newTier: 'Silver',
        direction: 'up',
        qualifyingPoints: 1000,
        transactionId: validEarnEntry.transactionId,
      },
    };
    expect(contract.validateEvent(event).errors.join('\n')).toMatch(/\/data\/direction must be equal to one of/);
  });
});

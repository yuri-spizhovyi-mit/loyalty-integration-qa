import { randomUUID } from 'node:crypto';
import { aCustomer, uniqueCustomerId } from '../../src/builders/customer-builder.js';
import type { CustomerResponse, ErrorResponse } from '../../src/clients/loyalty-types.js';
import { config } from '../../src/config.js';
import { expect, test } from '../../src/fixtures.js';
import { ISO_8601_UTC, readJson } from '../../src/helpers/json.js';

test.describe('Customers', () => {
  test('a new customer starts in Bronze with a zero balance', async ({ loyalty, createCustomer }) => {
    const { customerId } = await createCustomer();

    const response = await loyalty.getCustomer(customerId);

    await expect(response).toHaveStatus(200);
    const body = await readJson<CustomerResponse>(response);
    expect(body).toMatchObject({ customerId, balance: 0, qualifyingPoints: 0, tier: 'Bronze', marketingConsent: false });
    expect(body.createdAt).toMatch(ISO_8601_UTC);
  });

  test('create returns 201 with the stored customer', async ({ loyalty }) => {
    const body = aCustomer().withMarketingConsent().build();
    test.info().annotations.push({ type: 'customerId', description: body.customerId });
    // Raw client call: the createCustomer fixture would assert 201 itself.
    const response = await loyalty.createCustomer(body);

    await expect(response).toHaveStatus(201);
    expect(await readJson<CustomerResponse>(response)).toMatchObject({
      customerId: body.customerId,
      marketingConsent: true,
      tier: 'Bronze',
    });
  });

  test('creating the same customer twice is rejected with 409', async ({ loyalty, customer }) => {
    const response = await loyalty.createCustomer(customer);

    await expect(response).toBeApiError(409, 'CUSTOMER_ALREADY_EXISTS');
  });

  const invalidIds: { name: string; customerId: unknown }[] = [
    { name: 'too short (2 chars)', customerId: 'ab' },
    { name: 'too long (65 chars)', customerId: 'a'.repeat(65) },
    { name: 'contains a space', customerId: 'qa customer' },
    { name: 'contains a slash', customerId: 'qa/customer' },
    { name: 'empty string', customerId: '' },
    { name: 'a number', customerId: 12345 },
    { name: 'null', customerId: null },
  ];
  for (const { name, customerId } of invalidIds) {
    test(`customerId that is ${name} is rejected with 400`, async ({ loyalty }) => {
      const response = await loyalty.createCustomer({ customerId });

      await expect(response).toBeApiError(400, 'VALIDATION_ERROR');
      const body = await readJson<ErrorResponse>(response);
      expect(body.error.details).toContainEqual(expect.objectContaining({ field: 'customerId' }));
    });
  }

  test('customerId of 64 characters (max) is accepted', async ({ createCustomer }) => {
    const longest = uniqueCustomerId().padEnd(64, 'x');
    expect(longest).toHaveLength(64);

    await createCustomer({ customerId: longest });
  });

  test('customerId of 3 characters (min) passes validation', async ({ loyalty }) => {
    // Only 36^3 three-character IDs exist, so on a long-lived server one may
    // already be taken. 201 and 409 both prove validation let it through.
    const shortest = randomUUID().replace(/-/g, '').slice(0, 3);
    test.info().annotations.push({ type: 'customerId', description: shortest });

    const response = await loyalty.createCustomer({ customerId: shortest });

    expect([201, 409], `status for customerId "${shortest}"`).toContain(response.status());
  });

  test('a missing customerId is rejected with 400', async ({ loyalty }) => {
    const response = await loyalty.createCustomer({ marketingConsent: true });

    await expect(response).toBeApiError(400, 'VALIDATION_ERROR');
  });

  test('an unknown field is rejected with 400 (strict contract)', async ({ loyalty }) => {
    const response = await loyalty.createCustomer({ customerId: uniqueCustomerId(), tier: 'Gold' });

    await expect(response).toBeApiError(400, 'VALIDATION_ERROR');
    const body = await readJson<ErrorResponse>(response);
    expect(body.error.details).toContainEqual({ field: 'tier', message: 'is not an allowed field' });
  });

  test('marketingConsent must be a boolean', async ({ loyalty }) => {
    const response = await loyalty.createCustomer({ customerId: uniqueCustomerId(), marketingConsent: 'yes' });

    await expect(response).toBeApiError(400, 'VALIDATION_ERROR');
  });

  test('an unknown customer returns 404 on every read endpoint', async ({ loyalty }) => {
    const unknown = uniqueCustomerId('missing');

    for (const response of [
      await loyalty.getCustomer(unknown),
      await loyalty.getTier(unknown),
      await loyalty.getLedger(unknown),
    ]) {
      await expect(response).toBeApiError(404, 'CUSTOMER_NOT_FOUND');
    }
  });
});

test.describe('Protocol behaviour', () => {
  test('the X-Correlation-ID sent by the client is echoed in headers and error bodies', async ({
    loyalty,
    correlationId,
  }) => {
    const response = await loyalty.getCustomer(uniqueCustomerId('missing'));

    expect(response.headers()['x-correlation-id']).toBe(correlationId);
    expect((await readJson<ErrorResponse>(response)).correlationId).toBe(correlationId);
  });

  test('a correlation ID is generated when the client sends none', async ({ playwright }) => {
    const bare = await playwright.request.newContext({ baseURL: config.loyaltyUrl });
    try {
      const response = await bare.get('/health');

      await expect(response).toHaveStatus(200);
      expect(response.headers()['x-correlation-id']).toMatch(/^corr_[0-9a-f-]{36}$/);
    } finally {
      await bare.dispose();
    }
  });

  test('malformed JSON is rejected with 400 MALFORMED_JSON', async ({ loyalty }) => {
    const response = await loyalty.postRaw('/customers', '{"customerId": "qa-broken",');

    await expect(response).toBeApiError(400, 'MALFORMED_JSON');
  });

  test('a JSON array body is rejected with 400', async ({ loyalty }) => {
    const response = await loyalty.postRaw('/customers', '[{"customerId": "qa-array"}]');

    await expect(response).toBeApiError(400, 'VALIDATION_ERROR');
  });

  test('an unknown route returns 404 with the standard error body', async ({ loyaltyApi }) => {
    const response = await loyaltyApi.get('/no-such-endpoint');

    await expect(response).toBeApiError(404, 'NOT_FOUND');
  });

  test('responses are JSON', async ({ loyalty }) => {
    const response = await loyalty.health();

    await expect(response).toHaveStatus(200);
    expect(response.headers()['content-type']).toContain('application/json');
  });
});

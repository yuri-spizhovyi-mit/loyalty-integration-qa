import { randomUUID } from 'node:crypto';
import { type APIRequest, type APIRequestContext, test as base } from '@playwright/test';
import { aCustomer, uniqueCustomerId } from './builders/customer-builder.js';
import { anEventStream, type EventStream } from './builders/event-builder.js';
import { anEarn } from './builders/transaction-builder.js';
import { CdpClient } from './clients/cdp-client.js';
import { LoyaltyClient } from './clients/loyalty-client.js';
import type { CreateCustomerBody, TransactionResponse } from './clients/loyalty-types.js';
import { config } from './config.js';
import { readJson } from './helpers/json.js';
import { expect } from './helpers/matchers.js';
import { QueueInspector } from './helpers/queue-inspector.js';
import { getContract } from './helpers/schema-validator.js';

interface Fixtures {
  /** Unique per test and sent on every request as X-Correlation-ID. */
  correlationId: string;
  loyaltyApi: APIRequestContext;
  loyalty: LoyaltyClient;
  /** Creates a customer and records its ID on the test, so failures name it. */
  createCustomer: (body?: CreateCustomerBody) => Promise<CreateCustomerBody>;
  /** A fresh Bronze customer with a zero balance. */
  customer: CreateCustomerBody;
  /** Setup step: earn points and fail fast (with context) if that does not work. */
  earnPoints: (customerId: string, points: number) => Promise<TransactionResponse>;
  /** Customer data platform client; responses checked against cdp-openapi.yaml. */
  cdp: CdpClient;
  /** Publishes straight to the broker and inspects deliveries and dead letters. */
  queues: QueueInspector;
  /**
   * Builds events for a synthetic customer that exists only in the event
   * stream (not in the engine), for consumer tests that publish to the broker
   * directly. Any fault injected for that customer is cleared afterwards.
   */
  eventStream: EventStream;
  /** Prints the correlation and customer IDs when a test fails. */
  failureContext: void;
}

export const test = base.extend<Fixtures>({
  correlationId: async ({}, use, testInfo) => {
    const correlationId = `qa-${testInfo.testId.slice(0, 8)}-${randomUUID().slice(0, 8)}`;
    testInfo.annotations.push({ type: 'correlationId', description: correlationId });
    await use(correlationId);
  },

  loyaltyApi: async ({ playwright, correlationId }, use) => {
    const context = await apiContext(playwright.request, config.loyaltyUrl, correlationId);
    await use(context);
    await context.dispose();
  },

  // Every response through this client is validated against openapi.yaml.
  loyalty: async ({ loyaltyApi }, use) => {
    await use(new LoyaltyClient(loyaltyApi, getContract()));
  },

  createCustomer: async ({ loyalty }, use, testInfo) => {
    await use(async (body = aCustomer().build()) => {
      testInfo.annotations.push({ type: 'customerId', description: body.customerId });
      const response = await loyalty.createCustomer(body);
      await expect(response, `setup: create customer ${body.customerId}`).toHaveStatus(201);
      return body;
    });
  },

  customer: async ({ createCustomer }, use) => {
    await use(await createCustomer());
  },

  cdp: async ({ playwright, correlationId }, use) => {
    const context = await apiContext(playwright.request, config.cdpUrl, correlationId);
    await use(new CdpClient(context, getContract()));
    await context.dispose();
  },

  queues: async ({ playwright, correlationId }, use) => {
    const context = await apiContext(playwright.request, config.brokerUrl, correlationId);
    await use(new QueueInspector(context));
    await context.dispose();
  },

  eventStream: async ({ cdp, correlationId }, use, testInfo) => {
    const customerId = uniqueCustomerId('evt');
    testInfo.annotations.push({ type: 'customerId', description: customerId });
    await use(anEventStream(customerId, correlationId));
    await cdp.clearFault(customerId);
  },

  earnPoints: async ({ loyalty }, use) => {
    await use(async (customerId, points) => {
      const response = await loyalty.earn(customerId, anEarn().points(points).build());
      await expect(response, `setup: earn ${points} points for ${customerId}`).toHaveStatus(201);
      return readJson<TransactionResponse>(response);
    });
  },

  failureContext: [
    async ({ correlationId }, use, testInfo) => {
      await use();
      if (testInfo.status === testInfo.expectedStatus) return;
      const customers = testInfo.annotations
        .filter((annotation) => annotation.type === 'customerId')
        .map((annotation) => annotation.description)
        .join(', ');
      const line = `[test context] correlationId=${correlationId} customerId=${customers || '(none)'}`;
      console.error(line);
      await testInfo.attach('test-context', { body: line, contentType: 'text/plain' });
    },
    { auto: true },
  ],
});

function apiContext(
  request: APIRequest,
  baseURL: string,
  correlationId: string,
): Promise<APIRequestContext> {
  return request.newContext({
    baseURL,
    extraHTTPHeaders: { 'X-Correlation-ID': correlationId, Accept: 'application/json' },
  });
}

export { expect };

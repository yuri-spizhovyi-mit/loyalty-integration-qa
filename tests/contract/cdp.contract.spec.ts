import type { APIResponse } from '@playwright/test';
import { uniqueCustomerId } from '../../src/builders/customer-builder.js';
import { anEventStream, type EventStream } from '../../src/builders/event-builder.js';
import { invalidEvents } from '../../src/builders/invalid-events.js';
import type { CdpClient } from '../../src/clients/cdp-client.js';
import { expect, test } from '../../src/fixtures.js';
import { readJson } from '../../src/helpers/json.js';
import { getContract } from '../../src/helpers/schema-validator.js';

/**
 * Customer data platform against cdp-openapi.yaml, and the consumer against
 * asyncapi.yaml: it must accept exactly the events the spec allows. Events
 * are sent straight to the consumer here, without the broker.
 */

type Scenario = (ctx: { cdp: CdpClient; eventStream: EventStream }) => Promise<APIResponse>;

const scenarios: Record<string, Scenario> = {
  'getCdpHealth 200': ({ cdp }) => cdp.health(),

  'receiveEvent 200': ({ cdp, eventStream }) => cdp.sendEvent(JSON.stringify(eventStream.customerCreated())),
  'receiveEvent 400': ({ cdp }) => cdp.sendEvent('not json'),
  'receiveEvent 503': async ({ cdp, eventStream }) => {
    await cdp.setFault(eventStream.customerId, 'fail-before-apply', 1);
    return cdp.sendEvent(JSON.stringify(eventStream.customerCreated()));
  },

  'getProfile 200': async ({ cdp, eventStream }) => {
    await cdp.sendEvent(JSON.stringify(eventStream.customerCreated()));
    return cdp.getProfile(eventStream.customerId);
  },
  'getProfile 404': ({ cdp }) => cdp.getProfile(uniqueCustomerId('missing')),

  'setFault 204': ({ cdp, eventStream }) => cdp.setFault(eventStream.customerId, 'fail-after-apply', 1),
  'setFault 400': ({ cdp, eventStream }) => cdp.setFault(eventStream.customerId, 'explode', 0),
  'clearFault 204': ({ cdp, eventStream }) => cdp.clearFault(eventStream.customerId),
};

test.describe('Customer data platform responses match cdp-openapi.yaml', () => {
  for (const [key, scenario] of Object.entries(scenarios)) {
    const [operationId, status] = key.split(' ') as [string, string];
    test(key, async ({ cdp, eventStream }) => {
      const response = await scenario({ cdp, eventStream });

      await expect(response).toHaveStatus(Number(status));
      await expect(response).toMatchContract(operationId);
    });
  }

  test('every documented response has a scenario above', () => {
    const documented = getContract()
      .operations('cdp')
      .flatMap(({ operationId, statuses }) => statuses.map((status) => `${operationId} ${status}`))
      .sort();

    expect(Object.keys(scenarios).sort()).toEqual(documented);
  });
});

test.describe('Consumer accepts exactly what asyncapi.yaml allows', () => {
  test('every valid event type is valid in the spec and accepted by the consumer', async ({ cdp, eventStream }) => {
    const created = eventStream.customerCreated();
    const earned = eventStream.earned(1200);
    const upgrade = eventStream.tierChanged('Bronze', 'Silver', 1200, earned);
    const redeemed = eventStream.redeemed(100);
    const refunded = eventStream.refunded(200, earned);
    const events = [created, earned, upgrade, redeemed, refunded];
    expect(new Set(events.map((event) => event.type))).toEqual(new Set(getContract().eventTypes()));

    for (const event of events) {
      expect(event as unknown).toMatchEventSchema();
      const response = await cdp.sendEvent(JSON.stringify(event));
      await expect(response, event.type).toHaveStatus(200);
      expect(await readJson<{ result: string }>(response)).toMatchObject({ result: 'applied' });
    }
  });

  // The names come from a throwaway stream; each test builds its own events.
  const names = invalidEvents(anEventStream('names-only', 'n/a')).map((c) => c.name);

  for (const name of names) {
    test(`spec and consumer both reject: ${name}`, async ({ cdp, eventStream }) => {
      const invalid = invalidEvents(eventStream).find((candidate) => candidate.name === name)!;
      const raw = typeof invalid.body === 'string' ? invalid.body : JSON.stringify(invalid.body);

      if (typeof invalid.body !== 'string') expect(invalid.body).not.toMatchEventSchema();
      const response = await cdp.sendEvent(raw);
      await expect(response).toBeApiError(400, invalid.code);
    });
  }
});

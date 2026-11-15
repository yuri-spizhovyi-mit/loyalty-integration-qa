import type { APIResponse } from '@playwright/test';
import { uniqueCustomerId } from '../../src/builders/customer-builder.js';
import { aProfileTierChanged } from '../../src/builders/profile-event-builder.js';
import type { ProfileTier } from '../../src/clients/cdp-types.js';
import type { NotificationClient } from '../../src/clients/notification-client.js';
import type { Decision } from '../../src/clients/notification-types.js';
import { expect, test } from '../../src/fixtures.js';
import { readJson } from '../../src/helpers/json.js';
import { getContract } from '../../src/helpers/schema-validator.js';

/**
 * Notification service against notification-openapi.yaml, and its consumer
 * against cdp-asyncapi.yaml: it must accept exactly the events the spec
 * allows. Events go straight to the service here, without the broker.
 */

type Ctx = { notifications: NotificationClient; customerId: string; correlationId: string };
type Scenario = (ctx: Ctx) => Promise<APIResponse>;

const event = ({ customerId, correlationId }: Ctx) => aProfileTierChanged(customerId, correlationId);

const scenarios: Record<string, Scenario> = {
  'getNotificationHealth 200': ({ notifications }) => notifications.health(),

  'receiveProfileEvent 200': (ctx) => ctx.notifications.sendEvent(JSON.stringify(event(ctx).build())),
  'receiveProfileEvent 400': ({ notifications }) => notifications.sendEvent('not json'),
  'receiveProfileEvent 503': async (ctx) => {
    await ctx.notifications.setFault(ctx.customerId, 'fail-before-apply', 1);
    return ctx.notifications.sendEvent(JSON.stringify(event(ctx).build()));
  },

  'listNotifications 200': async (ctx) => {
    await ctx.notifications.sendEvent(JSON.stringify(event(ctx).build()));
    return ctx.notifications.listNotifications(ctx.customerId);
  },
  'listDecisions 200': async (ctx) => {
    await ctx.notifications.sendEvent(JSON.stringify(event(ctx).withConsent(false).build()));
    return ctx.notifications.listDecisions(ctx.customerId);
  },

  'setNotificationFault 204': ({ notifications, customerId }) => notifications.setFault(customerId, 'fail-after-apply', 1),
  'setNotificationFault 400': ({ notifications, customerId }) => notifications.setFault(customerId, 'explode', 0),
  'clearNotificationFault 204': ({ notifications, customerId }) => notifications.clearFault(customerId),
};

test.describe('Notification service responses match notification-openapi.yaml', () => {
  for (const [key, scenario] of Object.entries(scenarios)) {
    const [operationId, status] = key.split(' ') as [string, string];
    test(key, async ({ notifications, eventStream, correlationId }) => {
      const response = await scenario({ notifications, customerId: eventStream.customerId, correlationId });

      await expect(response).toHaveStatus(Number(status));
      await expect(response).toMatchContract(operationId);
    });
  }

  test('every documented response has a scenario above', () => {
    const documented = getContract()
      .operations('notification')
      .flatMap(({ operationId, statuses }) => statuses.map((status) => `${operationId} ${status}`))
      .sort();

    expect(Object.keys(scenarios).sort()).toEqual(documented);
  });

  test('an unknown customer has empty lists, not a 404', async ({ notifications }) => {
    const customerId = uniqueCustomerId('none');
    expect(await notifications.notifications(customerId)).toEqual([]);
    expect(await notifications.decisions(customerId)).toEqual([]);
  });
});

test.describe('Notification consumer accepts exactly what cdp-asyncapi.yaml allows', () => {
  const moves: [ProfileTier, ProfileTier][] = [
    ['bronze', 'silver'],
    ['bronze', 'gold'],
    ['silver', 'gold'],
    ['silver', 'bronze'],
    ['gold', 'silver'],
    ['gold', 'bronze'],
  ];
  for (const [from, to] of moves) {
    test(`a ${from} -> ${to} event is valid in the spec and accepted`, async ({ notifications, eventStream, correlationId }) => {
      const valid = aProfileTierChanged(eventStream.customerId, correlationId).from(from).to(to).build();
      expect(valid as unknown).toMatchEventSchema();

      const response = await notifications.sendEvent(JSON.stringify(valid));
      await expect(response).toHaveStatus(200);
      expect(await readJson<Decision>(response)).toMatchObject({ outcome: 'sent', sourceEventId: valid.sourceEventId });
    });
  }

  test('every consent value the spec allows is accepted (true, false, null)', async ({
    notifications,
    eventStream,
    correlationId,
  }) => {
    for (const consent of [true, false, null]) {
      const valid = aProfileTierChanged(eventStream.customerId, correlationId).withConsent(consent).build();
      expect(valid as unknown, `consent ${consent}`).toMatchEventSchema();
      await expect(await notifications.sendEvent(JSON.stringify(valid)), `consent ${consent}`).toHaveStatus(200);
    }
  });

  type Mutation = (event: Record<string, any>) => void;
  const invalid: { name: string; mutate: Mutation; code?: string }[] = [
    { name: 'an unknown type', mutate: (e) => (e.type = 'profile.deleted'), code: 'UNKNOWN_EVENT_TYPE' },
    { name: 'an engine event type', mutate: (e) => (e.type = 'tier.changed'), code: 'UNKNOWN_EVENT_TYPE' },
    { name: 'no sourceEventId', mutate: (e) => delete e.sourceEventId },
    { name: 'a sourceEventId that is not a UUID', mutate: (e) => (e.sourceEventId = 'evt-1') },
    { name: 'an extra envelope field', mutate: (e) => (e.sequence = 3) },
    { name: 'an engine tier name ("Silver")', mutate: (e) => (e.data.newTier = 'Silver') },
    { name: 'an unchanged tier', mutate: (e) => (e.data.newTier = e.data.previousTier) },
    { name: 'a direction that contradicts the tiers', mutate: (e) => (e.data.direction = 'downgrade') },
    { name: 'consent as a string', mutate: (e) => (e.data.marketingConsent = 'yes') },
    { name: 'consent missing', mutate: (e) => delete e.data.marketingConsent },
    { name: 'a malformed transactionId', mutate: (e) => (e.data.transactionId = 'txn_1') },
    { name: 'occurredAt not in UTC', mutate: (e) => (e.occurredAt = '2026-11-14T10:15:00+02:00') },
  ];
  for (const { name, mutate, code = 'INVALID_EVENT' } of invalid) {
    test(`spec and consumer both reject: ${name}`, async ({ notifications, eventStream, correlationId }) => {
      const bad = aProfileTierChanged(eventStream.customerId, correlationId).build() as unknown as Record<string, any>;
      mutate(bad);

      expect(bad).not.toMatchEventSchema();
      await expect(await notifications.sendEvent(JSON.stringify(bad))).toBeApiError(400, code);
      expect(await notifications.decisions(eventStream.customerId), 'a rejected event gets no decision').toEqual([]);
    });
  }
});

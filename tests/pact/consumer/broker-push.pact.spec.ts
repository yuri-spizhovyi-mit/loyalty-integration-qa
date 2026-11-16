import { MatchersV3 as M, PactV4 } from '@pact-foundation/pact';
import { expect, test } from '@playwright/test';
import { pushToSubscriber } from '../../../services/broker/src/broker.js';
import { EXAMPLE, loyaltyEnvelope, match, PACT_DIR, PARTICIPANTS, resetPact } from '../../../src/pact/contracts.js';

/**
 * The in-memory broker's contract with its two push subscribers. The broker
 * decides what to do with a message from the HTTP status alone:
 *   2xx -> delivered; 400 -> dead-letter at once; 503 -> retry with backoff.
 * If a subscriber answered 500 to a message it can never read, the broker
 * would retry a poison message; if it answered 400 to a temporary failure,
 * the broker would drop a good one. These pacts pin those three answers.
 */

const TIERS = ['bronze', 'silver', 'gold'] as const;

const subscribers = [
  {
    provider: PARTICIPANTS.cdp,
    validEvent: loyaltyEnvelope('customer.created', 1, { tier: 'Bronze', marketingConsent: match.boolean(true) }),
  },
  {
    provider: PARTICIPANTS.notifications,
    validEvent: {
      eventId: match.uuid(EXAMPLE.eventId),
      type: 'profile.tier_changed',
      customerId: match.customerId(),
      correlationId: match.correlationId(),
      occurredAt: match.timestamp(),
      sourceEventId: match.uuid(EXAMPLE.sourceEventId),
      data: {
        previousTier: match.oneOf(TIERS, 'bronze'),
        newTier: match.oneOf(TIERS, 'silver'),
        direction: match.oneOf(['upgrade', 'downgrade'], 'upgrade'),
        marketingConsent: match.boolean(true),
        transactionId: match.transactionId(),
      },
    },
  },
];

/** An event no subscriber can ever read: the broker must not retry it. */
const UNREADABLE = { eventId: 'not-a-uuid', type: 'points.earned', customerId: EXAMPLE.customerId };

const pushHeaders = {
  'Content-Type': 'application/json',
  'X-Message-Id': M.string('msg-1'),
  'X-Delivery-Attempt': M.regex('^[1-9][0-9]*$', '1'),
  'X-Correlation-ID': match.correlationId(),
};

for (const { provider, validEvent } of subscribers) {
  test.describe(`broker -> ${provider}`, () => {
    const pact = new PactV4({ consumer: PARTICIPANTS.broker, provider, dir: PACT_DIR, logLevel: 'warn' });
    test.beforeAll(() => resetPact(PARTICIPANTS.broker, provider));

    const push = (baseUrl: string, payload: unknown) =>
      pushToSubscriber(
        `${baseUrl}/events`,
        { messageId: 'msg-1', correlationId: EXAMPLE.correlationId, payload: JSON.stringify(payload) },
        1,
        2000,
      );

    test('an event it can process is acknowledged: delivered', async () => {
      await pact
        .addInteraction()
        .uponReceiving('a pushed event it can process')
        .withRequest('POST', '/events', (request) => request.headers(pushHeaders).jsonBody(validEvent))
        .willRespondWith(200)
        .executeTest(async (server) => {
          const attempt = await push(server.url, M.reify(validEvent));
          expect(attempt).toMatchObject({ status: 200, outcome: 'ack' });
        });
    });

    test('an event it can never read is answered 400: dead-lettered at once, not retried', async () => {
      await pact
        .addInteraction()
        .uponReceiving('a pushed event it cannot read')
        .withRequest('POST', '/events', (request) => request.headers(pushHeaders).jsonBody(UNREADABLE))
        .willRespondWith(400, (response) =>
          response.jsonBody({ error: { code: match.oneOf(['INVALID_EVENT', 'UNKNOWN_EVENT_TYPE', 'MALFORMED_JSON'], 'INVALID_EVENT') } }),
        )
        .executeTest(async (server) => {
          const attempt = await push(server.url, UNREADABLE);
          expect(attempt).toMatchObject({ status: 400, outcome: 'reject' });
        });
    });

    test('a temporary failure is answered 503: retried', async () => {
      await pact
        .addInteraction()
        .given('the subscriber cannot process events for the customer right now', { customerId: EXAMPLE.customerId })
        .uponReceiving('a pushed event during a temporary failure')
        .withRequest('POST', '/events', (request) => request.headers(pushHeaders).jsonBody(validEvent))
        .willRespondWith(503)
        .executeTest(async (server) => {
          const attempt = await push(server.url, M.reify(validEvent));
          expect(attempt).toMatchObject({ status: 503, outcome: 'retry' });
        });
    });
  });
}

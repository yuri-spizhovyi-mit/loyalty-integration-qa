import { expect, test } from '@playwright/test';
import { getContract } from '../../../src/helpers/schema-validator.js';
import { PARTICIPANTS, readPact } from '../../../src/pact/contracts.js';

/**
 * Coverage guards for the pacts: a new event type in a spec, or a new
 * subscriber answer the broker relies on, must come with a contract, or this
 * fails. Runs after the consumer tests have written the pact files.
 */

const typesIn = (consumer: string, provider: string) =>
  new Set(readPact(consumer, provider).interactions.map((i) => (i.contents?.content as { type?: string }).type));

test('every loyalty event in asyncapi.yaml has an interaction in the CDP pact', () => {
  expect(typesIn(PARTICIPANTS.cdp, PARTICIPANTS.engine)).toEqual(new Set(getContract().eventTypes('loyalty')));
});

test('every profile event in cdp-asyncapi.yaml has an interaction in the notification pact', () => {
  expect(typesIn(PARTICIPANTS.notifications, PARTICIPANTS.cdp)).toEqual(new Set(getContract().eventTypes('profile')));
});

test('the notification pact covers every consent value: true, false and unknown', () => {
  const consents = readPact(PARTICIPANTS.notifications, PARTICIPANTS.cdp).interactions.map(
    (i) => (i.contents?.content as { data: { marketingConsent: unknown } }).data.marketingConsent,
  );
  expect(new Set(consents)).toEqual(new Set([true, false, null]));
});

for (const provider of [PARTICIPANTS.cdp, PARTICIPANTS.notifications]) {
  test(`the broker pact with ${provider} covers ack, dead-letter and retry (200, 400, 503)`, () => {
    const statuses = readPact(PARTICIPANTS.broker, provider).interactions.map((i) => i.response?.status);
    expect(new Set(statuses)).toEqual(new Set([200, 400, 503]));
  });
}

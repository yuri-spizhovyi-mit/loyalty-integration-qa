import { MessageProviderPact } from '@pact-foundation/pact';
import { test } from '@playwright/test';
import { pactFile, PARTICIPANTS } from '../../../src/pact/contracts.js';
import { earn, engineEvents, lastOfType } from '../../../src/pact/engine-scenarios.js';

/**
 * Verifies the loyalty engine against what the customer data platform
 * expects of its events. Every message is produced by the engine's real
 * service code (ledger rules, tier calculation, event building), not by a
 * hand-written copy, and the verifier compares it with the pact.
 */
test('the loyalty engine publishes the events the customer data platform relies on', async () => {
  test.setTimeout(60_000);
  await new MessageProviderPact({
    provider: PARTICIPANTS.engine,
    pactUrls: [pactFile(PARTICIPANTS.cdp, PARTICIPANTS.engine)],
    logLevel: 'warn',
    messageProviders: {
      'a customer.created event': async () => lastOfType(await engineEvents(async () => {}), 'customer.created'),
      'a points.earned event': async () =>
        lastOfType(await engineEvents((engine, id, corr) => earn(engine, id, corr, 250)), 'points.earned'),
      'a points.redeemed event': async () =>
        lastOfType(
          await engineEvents(async (engine, id, corr) => {
            await earn(engine, id, corr, 250);
            await engine.redeem(id, { points: 100, reference: null }, corr);
          }),
          'points.redeemed',
        ),
      'a points.refunded event': async () =>
        lastOfType(
          await engineEvents(async (engine, id, corr) => {
            const { transaction } = await earn(engine, id, corr, 250);
            await engine.refund(id, { transactionId: transaction.transactionId, points: null, reference: null }, corr);
          }),
          'points.refunded',
        ),
      'a tier.changed event': async () =>
        lastOfType(await engineEvents((engine, id, corr) => earn(engine, id, corr, 1000)), 'tier.changed'),
    },
  }).verify();
});

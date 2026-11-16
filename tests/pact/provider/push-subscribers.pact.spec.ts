import type { AddressInfo } from 'node:net';
import type { Express } from 'express';
import { Verifier } from '@pact-foundation/pact';
import { test } from '@playwright/test';
import { createApp as createCdpApp } from '../../../services/customer-data-platform/src/app.js';
import { FaultInjector as CdpFaults } from '../../../services/customer-data-platform/src/faults.js';
import { ProfileStore } from '../../../services/customer-data-platform/src/profiles.js';
import { createApp as createNotificationApp } from '../../../services/notification-service/src/app.js';
import { FaultInjector as NotificationFaults } from '../../../services/notification-service/src/faults.js';
import { NotificationStore } from '../../../services/notification-service/src/notifications.js';
import { pactFile, PARTICIPANTS } from '../../../src/pact/contracts.js';

/**
 * Verifies the push endpoints of the customer data platform and the
 * notification service against the broker's pacts. Each service's real
 * Express app runs in-process on a free port; the Pact verifier replays the
 * broker's requests against it. The provider state "cannot process events"
 * uses the services' own fault injection.
 */

interface Faults {
  set(customerId: string, fault: { mode: 'fail-before-apply'; remaining: number }): void;
  clear(customerId: string): void;
}

async function verify(provider: string, app: Express, faults: Faults) {
  const server = app.listen(0);
  await new Promise((resolve) => server.once('listening', resolve));
  try {
    await new Verifier({
      provider,
      providerBaseUrl: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
      pactUrls: [pactFile(PARTICIPANTS.broker, provider)],
      logLevel: 'warn',
      stateHandlers: {
        'the subscriber cannot process events for the customer right now': {
          setup: async (params) => {
            faults.set((params as { customerId: string }).customerId, { mode: 'fail-before-apply', remaining: 1 });
          },
          teardown: async (params) => {
            faults.clear((params as { customerId: string }).customerId);
          },
        },
      },
    }).verifyProvider();
  } finally {
    server.close();
  }
}

test('the customer data platform answers pushed events as the broker expects', async () => {
  test.setTimeout(60_000);
  const faults = new CdpFaults();
  // Profile events are not under test here; drop them quietly.
  const app = createCdpApp(new ProfileStore(), faults, { publish: () => {} });
  await verify(PARTICIPANTS.cdp, app, faults);
});

test('the notification service answers pushed events as the broker expects', async () => {
  test.setTimeout(60_000);
  const faults = new NotificationFaults();
  await verify(PARTICIPANTS.notifications, createNotificationApp(new NotificationStore(), faults), faults);
});

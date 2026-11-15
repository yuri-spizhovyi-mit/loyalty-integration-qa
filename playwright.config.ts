import { defineConfig } from '@playwright/test';
import { config } from './src/config.js';

const isCI = Boolean(process.env.CI);
// When LOYALTY_URL is set the stack is already running (Docker Compose, a
// shared environment); otherwise Playwright starts the mock services itself.
const startServices = !process.env.LOYALTY_URL;

const service = (name: string, url: string, env: Record<string, string> = {}) => ({
  command: `npm run start:${name}`,
  url: `${url}/health`,
  env,
  reuseExistingServer: !isCI,
  timeout: 30_000,
  stdout: 'ignore' as const,
  stderr: 'pipe' as const,
});

export default defineConfig({
  testDir: 'tests',
  fullyParallel: true,
  forbidOnly: isCI,
  // No retries: a test that passes on retry is a flaky test, and it should be
  // reported and fixed, not hidden.
  retries: 0,
  reporter: [['list'], ['html', { open: 'never' }]],
  // Contract checks run first. If the specs, the validator or a response
  // shape is broken, the behaviour tests would fail for the wrong reason, so
  // they are skipped and the contract failure is reported on its own.
  projects: [
    { name: 'contract', testDir: 'tests/contract' },
    { name: 'api', testDir: 'tests/api', dependencies: ['contract'] },
    { name: 'events', testDir: 'tests/events', dependencies: ['contract'] },
  ],
  // Start order does not matter: the engine's relay retries until the broker
  // is up, and the broker retries until the customer data platform is up.
  webServer: startServices
    ? [
        service('broker', config.brokerUrl, { CDP_URL: config.cdpUrl }),
        service('cdp', config.cdpUrl),
        service('loyalty', config.loyaltyUrl, { BROKER_URL: config.brokerUrl }),
      ]
    : undefined,
});

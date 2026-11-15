import { defineConfig } from '@playwright/test';
import { config } from './src/config.js';

const isCI = Boolean(process.env.CI);
// When LOYALTY_URL is set the stack is already running (Docker Compose, a
// shared environment); otherwise Playwright starts the mock service itself.
const startServices = !process.env.LOYALTY_URL;

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
  ],
  webServer: startServices
    ? [
        {
          command: 'npm run start:loyalty',
          url: `${config.loyaltyUrl}/health`,
          reuseExistingServer: !isCI,
          timeout: 30_000,
          stdout: 'ignore',
          stderr: 'pipe',
        },
      ]
    : undefined,
});

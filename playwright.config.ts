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
  projects: [{ name: 'api', testDir: 'tests/api' }],
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

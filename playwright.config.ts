import { defineConfig } from '@playwright/test';
import { config } from './src/config.js';

const isCI = Boolean(process.env.CI);
// When LOYALTY_URL is set the stack is already running (Docker Compose, a
// shared environment); otherwise Playwright starts the mock services itself.
const startServices = !process.env.LOYALTY_URL;
if (config.bus === 'kafka' && startServices) {
  throw new Error(
    'BUS=kafka needs the Kafka stack running: start it with docker-compose.kafka.yml and set LOYALTY_URL (see the README).',
  );
}

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
  // In CI: annotations on the commit (github), a JUnit file for tools that
  // read one, and the HTML report that the workflow uploads as an artifact.
  reporter: isCI
    ? [['github'], ['list'], ['html', { open: 'never' }], ['junit', { outputFile: 'test-results/junit.xml' }]]
    : [['list'], ['html', { open: 'never' }]],
  // Contract checks run first. If the specs, the validator or a response
  // shape is broken, the behaviour tests would fail for the wrong reason, so
  // they are skipped and the contract failure is reported on its own.
  projects: [
    { name: 'contract', testDir: 'tests/contract' },
    { name: 'api', testDir: 'tests/api', dependencies: ['contract'] },
    { name: 'events', testDir: 'tests/events', dependencies: ['contract'] },
    { name: 'integrity', testDir: 'tests/integrity', dependencies: ['contract'] },
    { name: 'notifications', testDir: 'tests/notifications', dependencies: ['contract'] },
    // Kafka-specific behaviour (partitions, offsets, DLT records). Only on
    // Kafka, and last and one test at a time: a retried record blocks its
    // whole partition, so another test's injected fault on a shared partition
    // would distort the timing these tests measure.
    ...(config.bus === 'kafka'
      ? [
          {
            name: 'kafka',
            testDir: 'tests/kafka',
            // Only the tests for the retry mode the stack runs in.
            testIgnore: config.kafkaRetryMode === 'topics' ? '**/blocking-retries.spec.ts' : '**/retry-topics.spec.ts',
            fullyParallel: false,
            workers: 1,
            dependencies: ['api', 'events', 'integrity', 'notifications'],
          },
        ]
      : []),
    // Kafka failures driven through Docker Compose. Last of all, alone:
    // they stop Kafka and crash or scale the CDP.
    ...(config.bus === 'kafka' && config.chaos
      ? [
          {
            name: 'chaos',
            testDir: 'tests/chaos',
            fullyParallel: false,
            workers: 1,
            timeout: 180_000,
            dependencies: ['kafka'],
          },
        ]
      : []),
  ],
  // Start order does not matter: the relays in the engine and the customer
  // data platform retry until the broker is up, and the broker retries until
  // its subscribers are up.
  webServer: startServices
    ? [
        service('broker', config.brokerUrl, { CDP_URL: config.cdpUrl, NOTIFICATION_URL: config.notificationUrl }),
        service('cdp', config.cdpUrl, { BROKER_URL: config.brokerUrl }),
        service('notifications', config.notificationUrl),
        service('loyalty', config.loyaltyUrl, { BROKER_URL: config.brokerUrl }),
      ]
    : undefined,
});

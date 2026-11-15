import { execFileSync } from 'node:child_process';
import { expect } from './matchers.js';

/**
 * Drives Docker Compose from a test, for the chaos tests: stop and start
 * Kafka, scale the CDP, read a container's restart count. Uses COMPOSE_FILE
 * when it is set (as in CI), otherwise the Kafka stack's two files.
 */
const DEFAULT_FILES = ['docker-compose.yml', 'docker-compose.kafka.yml'];

export function compose(...args: string[]): string {
  const files = process.env.COMPOSE_FILE ? [] : DEFAULT_FILES.flatMap((file) => ['-f', file]);
  return execFileSync('docker', ['compose', ...files, ...args], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
}

/** How many times Docker has restarted the service's container. */
export function restartCount(service: string): number {
  const id = compose('ps', '-q', service).trim();
  return Number(execFileSync('docker', ['inspect', '-f', '{{.RestartCount}}', id], { encoding: 'utf8' }).trim());
}

/** Polls a service's /health until it answers 200. */
export async function waitHealthy(baseUrl: string, timeout = 60_000): Promise<void> {
  await expect
    .poll(
      async () => {
        try {
          return (await fetch(`${baseUrl}/health`, { signal: AbortSignal.timeout(1000) })).status;
        } catch {
          return 0;
        }
      },
      { timeout, intervals: [250, 500, 1000], message: `${baseUrl} healthy` },
    )
    .toBe(200);
}

/** Kafka failure tests take longer than the normal sync limit: a restart, a rejoin, a session timeout. */
export const RECOVERY_TIMEOUT_MS = 60_000;
export const RECOVERY_POLL = { timeout: RECOVERY_TIMEOUT_MS, intervals: [250, 500, 1000] };

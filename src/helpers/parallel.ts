import type { APIResponse } from '@playwright/test';
import type { LoyaltyClient } from '../clients/loyalty-client.js';

/**
 * Sends `count` requests at the same moment. It first opens `count` keep-alive
 * connections with cheap health checks; without that, connection setup
 * spreads the burst out and the requests may not overlap on the server, which
 * lets a race condition slip through.
 */
export async function inParallel(
  client: LoyaltyClient,
  count: number,
  request: (index: number) => Promise<APIResponse>,
): Promise<APIResponse[]> {
  await Promise.all(Array.from({ length: count }, () => client.health()));
  return Promise.all(Array.from({ length: count }, (_, index) => request(index)));
}

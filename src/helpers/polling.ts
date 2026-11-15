/**
 * Polling settings for asynchronous checks. Tests never sleep for a fixed
 * time: they poll until the expected state appears, and fail with the last
 * value seen if it does not appear in time.
 */

/** How long an event may take from the engine to the customer profile. */
export const SYNC_SLA_MS = 3_000;

/** Defaults for `expect.poll`: check often at first, then back off. */
export const POLL: { timeout: number; intervals: number[] } = { timeout: SYNC_SLA_MS, intervals: [25, 50, 100, 250, 500] };

/**
 * Upper bound for the broker to give up on a message: four attempts with
 * 100/200/400 ms backoff, plus request time. Generous so CI stays stable.
 */
export const DEAD_LETTER_TIMEOUT_MS = 10_000;

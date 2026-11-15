import { ApiError } from './errors.js';

/**
 * Runs tasks for the same key one at a time. The engine takes one lock per
 * customer, so two parallel redeems cannot both read the same balance and
 * overdraw the account.
 */
export class KeyedMutex {
  private readonly tails = new Map<string, Promise<void>>();

  async run<T>(key: string, task: () => Promise<T>): Promise<T> {
    const previous = this.tails.get(key) ?? Promise.resolve();
    let release!: () => void;
    const current = new Promise<void>((resolve) => (release = resolve));
    const tail = previous.then(() => current);
    this.tails.set(key, tail);

    await previous;
    try {
      return await task();
    } finally {
      release();
      if (this.tails.get(key) === tail) this.tails.delete(key);
    }
  }
}

export interface StoredResponse {
  status: number;
  body: unknown;
}

interface IdempotencyRecord {
  fingerprint: string;
  pending?: Promise<StoredResponse>;
  response?: StoredResponse;
}

/**
 * Idempotency-Key handling:
 * - same key, same request, after success  -> the stored response is replayed
 * - same key, same request, still running  -> waits for the first one, then replays
 * - same key, different request            -> 409 IDEMPOTENCY_KEY_REUSED
 * - first attempt failed                   -> nothing is stored, so a retry runs again
 */
export class IdempotencyStore {
  private readonly records = new Map<string, IdempotencyRecord>();

  async run(
    key: string,
    fingerprint: string,
    execute: () => Promise<StoredResponse>,
  ): Promise<StoredResponse & { replayed: boolean }> {
    const existing = this.records.get(key);
    if (existing) {
      if (existing.fingerprint !== fingerprint) {
        throw new ApiError(
          'IDEMPOTENCY_KEY_REUSED',
          'This Idempotency-Key was already used with a different request',
        );
      }
      if (existing.response) return { ...existing.response, replayed: true };
      if (existing.pending) {
        await existing.pending.catch(() => undefined);
        return this.run(key, fingerprint, execute);
      }
    }

    // No await between the lookup above and this set, so two requests with
    // the same key cannot both get here.
    const record: IdempotencyRecord = { fingerprint };
    record.pending = execute();
    this.records.set(key, record);

    try {
      const response = await record.pending;
      record.response = response;
      delete record.pending;
      return { ...response, replayed: false };
    } catch (error) {
      this.records.delete(key);
      throw error;
    }
  }
}

/** Stable JSON for fingerprinting: object keys sorted at every level. */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`);
    return `{${entries.join(',')}}`;
  }
  return JSON.stringify(value) ?? 'null';
}

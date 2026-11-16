import { readFileSync, rmSync } from 'node:fs';
import path from 'node:path';
import { MatchersV3 as M } from '@pact-foundation/pact';

/**
 * Consumer-driven contracts (Pact). Each consumer states, in a consumer test,
 * the messages or answers it relies on. The test runs the consumer's own
 * handling code against those examples and writes a pact file to `pacts/`.
 * The provider is then verified against that file with its own producing
 * code. Pact files are committed, so a change to a contract shows up in
 * review, and CI fails if a test run changes them without a commit.
 *
 * Unlike the rest of the suite, these tests import service code: a consumer
 * test must run the consumer's handler, and a provider verification must run
 * the producer. Everything else stays black-box over HTTP.
 */

export const PACT_DIR = path.resolve('pacts');

export const PARTICIPANTS = {
  engine: 'loyalty-engine',
  cdp: 'customer-data-platform',
  notifications: 'notification-service',
  broker: 'broker',
} as const;

export function pactFile(consumer: string, provider: string): string {
  return path.join(PACT_DIR, `${consumer}-${provider}.json`);
}

/** A consumer test starts from an empty pact, so a removed interaction does not linger in the file. */
export function resetPact(consumer: string, provider: string): void {
  rmSync(pactFile(consumer, provider), { force: true });
}

export interface PactFile {
  interactions: Array<{
    type: string;
    description: string;
    contents?: { content: unknown };
    request?: { body?: { content: unknown } };
    response?: { status: number };
  }>;
}

export function readPact(consumer: string, provider: string): PactFile {
  return JSON.parse(readFileSync(pactFile(consumer, provider), 'utf8')) as PactFile;
}

/** Message contents as the consumer's handler receives them: the raw JSON text. */
export function rawContent(content: unknown): string {
  return Buffer.isBuffer(content) ? content.toString('utf8') : JSON.stringify(content);
}

// ---------------------------------------------------------------------------
// Matchers. Each one mirrors a check the consumer really makes, so the pact
// asks for no more than the consumer needs. Pact matches types and formats,
// not ranges: "points >= 1" and "balanceAfter >= 0" stay with the AsyncAPI
// schema tests (tests/contract), which check every event the engine emits.
// ---------------------------------------------------------------------------

export const EXAMPLE = {
  customerId: 'pact-customer-1',
  correlationId: 'pact-correlation-1',
  eventId: '6f1c2a8e-3b4d-4e5f-8a9b-0c1d2e3f4a5b',
  sourceEventId: '9a8b7c6d-5e4f-4a3b-9c2d-1e0f9a8b7c6d',
  transactionId: 'txn_1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d',
  earnTransactionId: 'txn_0f1e2d3c-4b5a-4968-8776-655443322110',
  occurredAt: '2026-11-15T10:00:00.000Z',
} as const;

const UTC_TIMESTAMP = '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,3})?Z$';
const TRANSACTION_ID = '^txn_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
const CUSTOMER_ID = '^[A-Za-z0-9_-]{3,64}$';

export const match = {
  uuid: (example: string) => M.uuid(example),
  timestamp: (example: string = EXAMPLE.occurredAt) => M.regex(UTC_TIMESTAMP, example),
  transactionId: (example: string = EXAMPLE.transactionId) => M.regex(TRANSACTION_ID, example),
  customerId: (example: string = EXAMPLE.customerId) => M.regex(CUSTOMER_ID, example),
  correlationId: (example: string = EXAMPLE.correlationId) => M.string(example),
  integer: (example: number) => M.integer(example),
  boolean: (example: boolean) => M.boolean(example),
  oneOf: (values: readonly string[], example: string) => M.regex(`^(${values.join('|')})$`, example),
  nullOr: (example: string | null) => (example === null ? null : M.string(example)),
};

/** The envelope every loyalty event shares (asyncapi.yaml). */
export function loyaltyEnvelope(type: string, sequence: number, data: Record<string, unknown>) {
  return {
    eventId: match.uuid(EXAMPLE.eventId),
    type,
    // customer.created must be sequence 1 exactly; the others only need an integer.
    sequence: type === 'customer.created' ? 1 : match.integer(sequence),
    customerId: match.customerId(),
    correlationId: match.correlationId(),
    occurredAt: match.timestamp(),
    data,
  };
}

/** The part of a Pact message a consumer test hands to the consumer's handler. */
export interface PactMessage {
  contents: { content: unknown };
}

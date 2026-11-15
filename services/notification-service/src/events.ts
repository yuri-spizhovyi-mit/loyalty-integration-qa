/**
 * The notification service's own check of incoming profile events, written
 * by hand like the customer data platform's, so the contract tests can
 * compare it with cdp-asyncapi.yaml. Anything rejected is answered with 400
 * and dead-lettered by the broker.
 */

export type ProfileTier = 'bronze' | 'silver' | 'gold';

export interface ProfileTierChanged {
  eventId: string;
  type: 'profile.tier_changed';
  customerId: string;
  correlationId: string;
  occurredAt: string;
  sourceEventId: string;
  data: {
    previousTier: ProfileTier;
    newTier: ProfileTier;
    direction: 'upgrade' | 'downgrade';
    marketingConsent: boolean | null;
    transactionId: string;
  };
}

export class InvalidEventError extends Error {
  constructor(
    readonly code: 'MALFORMED_JSON' | 'INVALID_EVENT' | 'UNKNOWN_EVENT_TYPE',
    readonly problems: string[],
  ) {
    super(problems.join('; '));
  }
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const CUSTOMER_ID = /^[A-Za-z0-9_-]{3,64}$/;
const TRANSACTION_ID = /^txn_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const UTC_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$/;
const TIERS: readonly string[] = ['bronze', 'silver', 'gold'];
const ENVELOPE_FIELDS = ['eventId', 'type', 'customerId', 'correlationId', 'occurredAt', 'sourceEventId', 'data'];
const DATA_FIELDS = ['previousTier', 'newTier', 'direction', 'marketingConsent', 'transactionId'];

type Obj = Record<string, unknown>;
const isObject = (value: unknown): value is Obj => typeof value === 'object' && value !== null && !Array.isArray(value);

function onlyFields(obj: Obj, allowed: string[], where: string, problems: string[]) {
  for (const key of Object.keys(obj)) if (!allowed.includes(key)) problems.push(`${where}${key} is not allowed`);
  for (const key of allowed) if (!(key in obj)) problems.push(`${where}${key} is required`);
}

function check(condition: boolean, problem: string, problems: string[]) {
  if (!condition) problems.push(problem);
}

export function parseEvent(raw: string): ProfileTierChanged {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new InvalidEventError('MALFORMED_JSON', ['body is not valid JSON']);
  }
  if (!isObject(parsed)) throw new InvalidEventError('INVALID_EVENT', ['event must be a JSON object']);
  if (parsed.type !== 'profile.tier_changed') {
    throw new InvalidEventError('UNKNOWN_EVENT_TYPE', [`type ${JSON.stringify(parsed.type)} is not a known event type`]);
  }

  const problems: string[] = [];
  onlyFields(parsed, ENVELOPE_FIELDS, '', problems);
  check(typeof parsed.eventId === 'string' && UUID.test(parsed.eventId), 'eventId must be a UUID', problems);
  check(typeof parsed.sourceEventId === 'string' && UUID.test(parsed.sourceEventId), 'sourceEventId must be a UUID', problems);
  check(typeof parsed.customerId === 'string' && CUSTOMER_ID.test(parsed.customerId), 'customerId is invalid', problems);
  check(
    typeof parsed.correlationId === 'string' && parsed.correlationId.length >= 1 && parsed.correlationId.length <= 128,
    'correlationId must be 1-128 characters',
    problems,
  );
  check(typeof parsed.occurredAt === 'string' && UTC_TIMESTAMP.test(parsed.occurredAt), 'occurredAt must be ISO 8601 UTC', problems);

  const data = parsed.data;
  if (!isObject(data)) {
    problems.push('data must be an object');
  } else {
    onlyFields(data, DATA_FIELDS, 'data.', problems);
    check(TIERS.includes(data.previousTier as string), 'data.previousTier is not a tier', problems);
    check(TIERS.includes(data.newTier as string), 'data.newTier is not a tier', problems);
    check(data.previousTier !== data.newTier, 'data.newTier must differ from data.previousTier', problems);
    const up = TIERS.indexOf(data.newTier as string) > TIERS.indexOf(data.previousTier as string);
    check(data.direction === (up ? 'upgrade' : 'downgrade'), 'data.direction does not match the tiers', problems);
    check(
      typeof data.marketingConsent === 'boolean' || data.marketingConsent === null,
      'data.marketingConsent must be a boolean or null',
      problems,
    );
    check(typeof data.transactionId === 'string' && TRANSACTION_ID.test(data.transactionId), 'data.transactionId is invalid', problems);
  }

  if (problems.length > 0) throw new InvalidEventError('INVALID_EVENT', problems);
  return parsed as unknown as ProfileTierChanged;
}

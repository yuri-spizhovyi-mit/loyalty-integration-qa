/**
 * The consumer's own check of incoming loyalty events. It is written by hand
 * (not generated from asyncapi.yaml) on purpose: the tests compare what this
 * consumer accepts with what the spec allows, which catches the two drifting
 * apart. Anything rejected here is answered with 400, which the broker treats
 * as permanent and dead-letters without retrying.
 */

export type Tier = 'Bronze' | 'Silver' | 'Gold';

interface Envelope<T extends string, D> {
  eventId: string;
  type: T;
  sequence: number;
  customerId: string;
  correlationId: string;
  occurredAt: string;
  data: D;
}

export interface TransactionData {
  transactionId: string;
  entryType: 'EARN' | 'REDEEM' | 'REFUND';
  points: number;
  balanceAfter: number;
  reference: string | null;
  refundOf: string | null;
}

export type LoyaltyEvent =
  | Envelope<'customer.created', { tier: Tier; marketingConsent: boolean }>
  | Envelope<'points.earned' | 'points.redeemed' | 'points.refunded', TransactionData>
  | Envelope<
      'tier.changed',
      { previousTier: Tier; newTier: Tier; direction: 'upgrade' | 'downgrade'; qualifyingPoints: number; transactionId: string }
    >;

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
const TIERS: readonly string[] = ['Bronze', 'Silver', 'Gold'];
const ENVELOPE_FIELDS = ['eventId', 'type', 'sequence', 'customerId', 'correlationId', 'occurredAt', 'data'];
const TRANSACTION_TYPES = {
  'points.earned': 'EARN',
  'points.redeemed': 'REDEEM',
  'points.refunded': 'REFUND',
} as const;

type Obj = Record<string, unknown>;
const isObject = (value: unknown): value is Obj => typeof value === 'object' && value !== null && !Array.isArray(value);
const isInt = (value: unknown): value is number => typeof value === 'number' && Number.isInteger(value);

function onlyFields(obj: Obj, allowed: string[], where: string, problems: string[]) {
  for (const key of Object.keys(obj)) if (!allowed.includes(key)) problems.push(`${where}${key} is not allowed`);
  for (const key of allowed) if (!(key in obj)) problems.push(`${where}${key} is required`);
}

function check(condition: boolean, problem: string, problems: string[]) {
  if (!condition) problems.push(problem);
}

export function parseEvent(raw: string): LoyaltyEvent {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new InvalidEventError('MALFORMED_JSON', ['body is not valid JSON']);
  }
  if (!isObject(parsed)) throw new InvalidEventError('INVALID_EVENT', ['event must be a JSON object']);

  const type = parsed.type;
  const known = ['customer.created', 'tier.changed', ...Object.keys(TRANSACTION_TYPES)];
  if (typeof type !== 'string' || !known.includes(type)) {
    throw new InvalidEventError('UNKNOWN_EVENT_TYPE', [`type ${JSON.stringify(type)} is not a known event type`]);
  }

  const problems: string[] = [];
  onlyFields(parsed, ENVELOPE_FIELDS, '', problems);
  check(typeof parsed.eventId === 'string' && UUID.test(parsed.eventId), 'eventId must be a UUID', problems);
  check(isInt(parsed.sequence) && parsed.sequence >= 1, 'sequence must be an integer >= 1', problems);
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
  } else if (type === 'customer.created') {
    onlyFields(data, ['tier', 'marketingConsent'], 'data.', problems);
    check(parsed.sequence === 1, 'customer.created must have sequence 1', problems);
    check(data.tier === 'Bronze', 'data.tier must be Bronze', problems);
    check(typeof data.marketingConsent === 'boolean', 'data.marketingConsent must be a boolean', problems);
  } else if (type === 'tier.changed') {
    onlyFields(data, ['previousTier', 'newTier', 'direction', 'qualifyingPoints', 'transactionId'], 'data.', problems);
    check(TIERS.includes(data.previousTier as string), 'data.previousTier is not a tier', problems);
    check(TIERS.includes(data.newTier as string), 'data.newTier is not a tier', problems);
    check(data.direction === 'upgrade' || data.direction === 'downgrade', 'data.direction is invalid', problems);
    check(isInt(data.qualifyingPoints) && data.qualifyingPoints >= 0, 'data.qualifyingPoints must be an integer >= 0', problems);
    check(typeof data.transactionId === 'string' && TRANSACTION_ID.test(data.transactionId), 'data.transactionId is invalid', problems);
  } else {
    const entryType = TRANSACTION_TYPES[type as keyof typeof TRANSACTION_TYPES];
    onlyFields(data, ['transactionId', 'entryType', 'points', 'balanceAfter', 'reference', 'refundOf'], 'data.', problems);
    check(typeof data.transactionId === 'string' && TRANSACTION_ID.test(data.transactionId), 'data.transactionId is invalid', problems);
    check(data.entryType === entryType, `data.entryType must be ${entryType}`, problems);
    const signOk = entryType === 'EARN' ? (data.points as number) >= 1 : (data.points as number) <= -1;
    check(isInt(data.points) && signOk && Math.abs(data.points) <= 1_000_000, 'data.points has the wrong sign or size', problems);
    check(isInt(data.balanceAfter) && data.balanceAfter >= 0, 'data.balanceAfter must be an integer >= 0', problems);
    check(
      data.reference === null || (typeof data.reference === 'string' && data.reference.length >= 1 && data.reference.length <= 128),
      'data.reference must be null or 1-128 characters',
      problems,
    );
    const refundOk =
      entryType === 'REFUND'
        ? typeof data.refundOf === 'string' && TRANSACTION_ID.test(data.refundOf)
        : data.refundOf === null;
    check(refundOk, 'data.refundOf is invalid for this entry type', problems);
  }

  if (problems.length > 0) throw new InvalidEventError('INVALID_EVENT', problems);
  return parsed as unknown as LoyaltyEvent;
}

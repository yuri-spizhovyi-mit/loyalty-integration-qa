import type { EventStream } from './event-builder.js';

export interface InvalidEventCase {
  name: string;
  /** An event object with one thing wrong, or a raw string that is not valid JSON. */
  body: unknown;
  /** The error code the consumer must answer with. */
  code: 'MALFORMED_JSON' | 'INVALID_EVENT' | 'UNKNOWN_EVENT_TYPE';
}

const strip = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

/**
 * Events that asyncapi.yaml does not allow, each broken in exactly one way.
 * Shared by the consumer/spec parity test and the dead-letter tests.
 */
export function invalidEvents(stream: EventStream): InvalidEventCase[] {
  const created = stream.customerCreated();
  const earned = stream.earned(100);
  const redeemed = stream.redeemed(10);
  const refunded = stream.refunded(10, earned);
  const upgrade = stream.tierChanged('Bronze', 'Silver', 1000, earned);
  return [
    { name: 'body is not JSON', body: '{"eventId": "abc", ', code: 'MALFORMED_JSON' },
    { name: 'body is a JSON array', body: [earned], code: 'INVALID_EVENT' },
    { name: 'unknown event type', body: { ...earned, type: 'points.expired' }, code: 'UNKNOWN_EVENT_TYPE' },
    { name: 'no eventId', body: strip({ ...earned, eventId: undefined }), code: 'INVALID_EVENT' },
    { name: 'eventId is not a UUID', body: { ...earned, eventId: 'evt-1' }, code: 'INVALID_EVENT' },
    { name: 'sequence 0', body: { ...earned, sequence: 0 }, code: 'INVALID_EVENT' },
    { name: 'no customerId', body: strip({ ...earned, customerId: undefined }), code: 'INVALID_EVENT' },
    { name: 'occurredAt not in UTC', body: { ...earned, occurredAt: '2026-11-14T10:15:00+02:00' }, code: 'INVALID_EVENT' },
    { name: 'an extra envelope field', body: { ...earned, retryCount: 1 }, code: 'INVALID_EVENT' },
    { name: 'an extra data field', body: { ...earned, data: { ...earned.data, tier: 'Gold' } }, code: 'INVALID_EVENT' },
    { name: 'points as a string', body: { ...earned, data: { ...earned.data, points: '100' } }, code: 'INVALID_EVENT' },
    { name: 'EARN with negative points', body: { ...earned, data: { ...earned.data, points: -100 } }, code: 'INVALID_EVENT' },
    { name: 'type and entryType disagree', body: { ...redeemed, type: 'points.earned' }, code: 'INVALID_EVENT' },
    { name: 'REFUND without refundOf', body: { ...refunded, data: { ...refunded.data, refundOf: null } }, code: 'INVALID_EVENT' },
    { name: 'tier in lower case', body: { ...upgrade, data: { ...upgrade.data, newTier: 'silver' } }, code: 'INVALID_EVENT' },
    { name: 'customer.created with sequence 2', body: { ...created, sequence: 2 }, code: 'INVALID_EVENT' },
  ];
}

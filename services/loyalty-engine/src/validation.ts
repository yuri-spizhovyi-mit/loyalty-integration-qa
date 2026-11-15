import { ApiError, type FieldError } from './errors.js';

export const MAX_POINTS_PER_TRANSACTION = 1_000_000;
export const CUSTOMER_ID_PATTERN = /^[A-Za-z0-9_-]{3,64}$/;
const MAX_REFERENCE_LENGTH = 128;

type Body = Record<string, unknown>;

function asObject(body: unknown): Body {
  if (typeof body !== 'object' || body === null || Array.isArray(body)) {
    throw new ApiError('VALIDATION_ERROR', 'Request body must be a JSON object', [
      { field: '(body)', message: 'must be a JSON object' },
    ]);
  }
  return body as Body;
}

function rejectUnknownFields(body: Body, allowed: readonly string[], errors: FieldError[]): void {
  for (const key of Object.keys(body)) {
    if (!allowed.includes(key)) errors.push({ field: key, message: 'is not an allowed field' });
  }
}

function checkPoints(value: unknown, field: string, errors: FieldError[]): void {
  if (typeof value !== 'number' || !Number.isInteger(value)) {
    errors.push({ field, message: 'must be an integer' });
  } else if (value < 1) {
    errors.push({ field, message: 'must be at least 1' });
  } else if (value > MAX_POINTS_PER_TRANSACTION) {
    errors.push({ field, message: `must be at most ${MAX_POINTS_PER_TRANSACTION}` });
  }
}

function checkReference(value: unknown, errors: FieldError[]): void {
  if (value === undefined) return;
  if (typeof value !== 'string' || value.length === 0 || value.length > MAX_REFERENCE_LENGTH) {
    errors.push({ field: 'reference', message: `must be a string of 1-${MAX_REFERENCE_LENGTH} characters` });
  }
}

function finish(errors: FieldError[]): void {
  if (errors.length > 0) throw new ApiError('VALIDATION_ERROR', 'Request body is invalid', errors);
}

export interface CreateCustomerRequest {
  customerId: string;
  marketingConsent: boolean;
}

export function parseCreateCustomer(raw: unknown): CreateCustomerRequest {
  const body = asObject(raw);
  const errors: FieldError[] = [];
  rejectUnknownFields(body, ['customerId', 'marketingConsent'], errors);
  if (typeof body.customerId !== 'string' || !CUSTOMER_ID_PATTERN.test(body.customerId)) {
    errors.push({ field: 'customerId', message: 'must be 3-64 characters: letters, digits, "-" or "_"' });
  }
  if (body.marketingConsent !== undefined && typeof body.marketingConsent !== 'boolean') {
    errors.push({ field: 'marketingConsent', message: 'must be a boolean' });
  }
  finish(errors);
  return { customerId: body.customerId as string, marketingConsent: (body.marketingConsent as boolean) ?? false };
}

export interface PointsRequest {
  points: number;
  reference: string | null;
}

export function parsePointsRequest(raw: unknown): PointsRequest {
  const body = asObject(raw);
  const errors: FieldError[] = [];
  rejectUnknownFields(body, ['points', 'reference'], errors);
  checkPoints(body.points, 'points', errors);
  checkReference(body.reference, errors);
  finish(errors);
  return { points: body.points as number, reference: (body.reference as string | undefined) ?? null };
}

export interface RefundRequest {
  transactionId: string;
  /** Omitted means "refund whatever is left of the original earn". */
  points: number | null;
  reference: string | null;
}

export function parseRefundRequest(raw: unknown): RefundRequest {
  const body = asObject(raw);
  const errors: FieldError[] = [];
  rejectUnknownFields(body, ['transactionId', 'points', 'reference'], errors);
  if (typeof body.transactionId !== 'string' || body.transactionId.length === 0) {
    errors.push({ field: 'transactionId', message: 'is required' });
  }
  if (body.points !== undefined) checkPoints(body.points, 'points', errors);
  checkReference(body.reference, errors);
  finish(errors);
  return {
    transactionId: body.transactionId as string,
    points: (body.points as number | undefined) ?? null,
    reference: (body.reference as string | undefined) ?? null,
  };
}

export function parseIdempotencyKey(header: string | undefined): string | null {
  if (header === undefined) return null;
  if (header.length < 1 || header.length > 255) {
    throw new ApiError('VALIDATION_ERROR', 'Idempotency-Key header is invalid', [
      { field: 'Idempotency-Key', message: 'must be 1-255 characters' },
    ]);
  }
  return header;
}

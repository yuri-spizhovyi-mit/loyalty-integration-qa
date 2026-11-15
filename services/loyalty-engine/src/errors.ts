export type ErrorCode =
  | 'VALIDATION_ERROR'
  | 'MALFORMED_JSON'
  | 'NOT_FOUND'
  | 'CUSTOMER_NOT_FOUND'
  | 'CUSTOMER_ALREADY_EXISTS'
  | 'TRANSACTION_NOT_FOUND'
  | 'INSUFFICIENT_BALANCE'
  | 'REFUND_NOT_ALLOWED'
  | 'REFUND_EXCEEDS_ORIGINAL'
  | 'IDEMPOTENCY_KEY_REUSED'
  | 'INTERNAL_ERROR';

export const STATUS_BY_CODE: Record<ErrorCode, number> = {
  VALIDATION_ERROR: 400,
  MALFORMED_JSON: 400,
  NOT_FOUND: 404,
  CUSTOMER_NOT_FOUND: 404,
  CUSTOMER_ALREADY_EXISTS: 409,
  TRANSACTION_NOT_FOUND: 404,
  INSUFFICIENT_BALANCE: 422,
  REFUND_NOT_ALLOWED: 422,
  REFUND_EXCEEDS_ORIGINAL: 422,
  IDEMPOTENCY_KEY_REUSED: 409,
  INTERNAL_ERROR: 500,
};

export interface FieldError {
  field: string;
  message: string;
}

export class ApiError extends Error {
  readonly status: number;
  constructor(
    readonly code: ErrorCode,
    message: string,
    readonly details: FieldError[] = [],
  ) {
    super(message);
    this.status = STATUS_BY_CODE[code];
  }
}

export function errorBody(error: ApiError, correlationId: string) {
  return {
    error: {
      code: error.code,
      message: error.message,
      ...(error.details.length > 0 ? { details: error.details } : {}),
    },
    correlationId,
  };
}

import type { APIResponse } from '@playwright/test';
import type { ContractValidator } from './schema-validator.js';

/** Thrown when a response does not match its OpenAPI document. */
export class ContractViolationError extends Error {
  override name = 'ContractViolationError';
}

/**
 * Awaits a response and validates it against the OpenAPI schema for its
 * operation and status before the test sees it. A wrong field, an extra field
 * or an undocumented status code fails the test right there, even if the test
 * itself only looks at one field. An empty body is checked as "no content".
 */
export async function checkedResponse(
  contract: ContractValidator | undefined,
  operationId: string,
  pending: Promise<APIResponse>,
): Promise<APIResponse> {
  const response = await pending;
  if (!contract) return response;

  const text = await response.text();
  let body: unknown;
  let parsed = true;
  if (text.length > 0) {
    try {
      body = JSON.parse(text);
    } catch {
      parsed = false;
    }
  }
  const result = parsed
    ? contract.validateResponse(operationId, response.status(), body)
    : { valid: false, against: `${operationId} ${response.status()}`, errors: ['response body is not JSON'] };
  if (!result.valid) {
    throw new ContractViolationError(
      [
        `Response does not match its OpenAPI document (${result.against})`,
        ...result.errors.map((error) => `  - ${error}`),
        `  url:           ${response.url()}`,
        `  correlationId: ${response.headers()['x-correlation-id'] ?? '(none)'}`,
        `  body:          ${text.slice(0, 2000)}`,
      ].join('\n'),
    );
  }
  return response;
}

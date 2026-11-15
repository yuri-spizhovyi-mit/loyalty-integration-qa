import type { APIRequestContext, APIResponse } from '@playwright/test';
import type { ContractValidator } from '../helpers/schema-validator.js';
import type { CreateCustomerBody, PointsBody, RefundBody } from './loyalty-types.js';

export interface WriteOptions {
  idempotencyKey?: string;
}

/** Thrown when a response does not match openapi.yaml. */
export class ContractViolationError extends Error {
  override name = 'ContractViolationError';
}

/**
 * Service object for the loyalty engine REST API. Methods return the raw
 * APIResponse so tests can assert on status, headers and body. Bodies are
 * typed loosely (`Record<string, unknown>` overloads) so negative tests can
 * send bad input.
 *
 * With a ContractValidator, every response is checked against the OpenAPI
 * schema for its operation and status before the test sees it. A wrong field,
 * an extra field or an undocumented status code fails the test right there,
 * even if the test itself only looks at one field.
 */
export class LoyaltyClient {
  constructor(
    private readonly request: APIRequestContext,
    private readonly contract?: ContractValidator,
  ) {}

  health(): Promise<APIResponse> {
    return this.checked('getHealth', this.request.get('/health'));
  }

  createCustomer(body: CreateCustomerBody | Record<string, unknown>): Promise<APIResponse> {
    return this.checked('createCustomer', this.request.post('/customers', { data: body }));
  }

  getCustomer(customerId: string): Promise<APIResponse> {
    return this.checked('getCustomer', this.request.get(`/customers/${encodeURIComponent(customerId)}`));
  }

  getTier(customerId: string): Promise<APIResponse> {
    return this.checked('getTier', this.request.get(`/customers/${encodeURIComponent(customerId)}/tier`));
  }

  getLedger(customerId: string): Promise<APIResponse> {
    return this.checked('getLedger', this.request.get(`/customers/${encodeURIComponent(customerId)}/ledger`));
  }

  getOutbox(customerId: string): Promise<APIResponse> {
    return this.checked('listOutboxEvents', this.request.get('/internal/outbox', { params: { customerId } }));
  }

  earn(customerId: string, body: PointsBody | Record<string, unknown>, options: WriteOptions = {}): Promise<APIResponse> {
    return this.write('earnPoints', `/customers/${encodeURIComponent(customerId)}/earn`, body, options);
  }

  redeem(customerId: string, body: PointsBody | Record<string, unknown>, options: WriteOptions = {}): Promise<APIResponse> {
    return this.write('redeemPoints', `/customers/${encodeURIComponent(customerId)}/redeem`, body, options);
  }

  refund(customerId: string, body: RefundBody | Record<string, unknown>, options: WriteOptions = {}): Promise<APIResponse> {
    return this.write('refundPoints', `/customers/${encodeURIComponent(customerId)}/refund`, body, options);
  }

  /** Sends a body exactly as given, for malformed-JSON tests. */
  postRaw(operationId: string, path: string, rawBody: string): Promise<APIResponse> {
    return this.checked(
      operationId,
      this.request.post(path, { data: rawBody, headers: { 'Content-Type': 'application/json' } }),
    );
  }

  private write(operationId: string, path: string, body: unknown, { idempotencyKey }: WriteOptions) {
    return this.checked(
      operationId,
      this.request.post(path, {
        data: body,
        headers: idempotencyKey === undefined ? {} : { 'Idempotency-Key': idempotencyKey },
      }),
    );
  }

  private async checked(operationId: string, pending: Promise<APIResponse>): Promise<APIResponse> {
    const response = await pending;
    if (!this.contract) return response;

    let body: unknown;
    try {
      body = await response.json();
    } catch {
      body = undefined;
    }
    const result =
      body === undefined
        ? { valid: false, against: `${operationId} ${response.status()}`, errors: ['response body is not JSON'] }
        : this.contract.validateResponse(operationId, response.status(), body);
    if (!result.valid) {
      throw new ContractViolationError(
        [
          `Response does not match openapi.yaml (${result.against})`,
          ...result.errors.map((error) => `  - ${error}`),
          `  url:           ${response.url()}`,
          `  correlationId: ${response.headers()['x-correlation-id'] ?? '(none)'}`,
          `  body:          ${(await response.text()).slice(0, 2000)}`,
        ].join('\n'),
      );
    }
    return response;
  }
}

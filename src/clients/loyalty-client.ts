import type { APIRequestContext, APIResponse } from '@playwright/test';
import type { CreateCustomerBody, PointsBody, RefundBody } from './loyalty-types.js';

export interface WriteOptions {
  idempotencyKey?: string;
}

/**
 * Service object for the loyalty engine REST API. Methods return the raw
 * APIResponse so tests can assert on status, headers and body. Bodies are
 * typed loosely (`unknown` overloads) so negative tests can send bad input.
 */
export class LoyaltyClient {
  constructor(private readonly request: APIRequestContext) {}

  health(): Promise<APIResponse> {
    return this.request.get('/health');
  }

  createCustomer(body: CreateCustomerBody | Record<string, unknown>): Promise<APIResponse> {
    return this.request.post('/customers', { data: body });
  }

  getCustomer(customerId: string): Promise<APIResponse> {
    return this.request.get(`/customers/${encodeURIComponent(customerId)}`);
  }

  getTier(customerId: string): Promise<APIResponse> {
    return this.request.get(`/customers/${encodeURIComponent(customerId)}/tier`);
  }

  getLedger(customerId: string): Promise<APIResponse> {
    return this.request.get(`/customers/${encodeURIComponent(customerId)}/ledger`);
  }

  earn(customerId: string, body: PointsBody | Record<string, unknown>, options: WriteOptions = {}): Promise<APIResponse> {
    return this.write(`/customers/${encodeURIComponent(customerId)}/earn`, body, options);
  }

  redeem(customerId: string, body: PointsBody | Record<string, unknown>, options: WriteOptions = {}): Promise<APIResponse> {
    return this.write(`/customers/${encodeURIComponent(customerId)}/redeem`, body, options);
  }

  refund(customerId: string, body: RefundBody | Record<string, unknown>, options: WriteOptions = {}): Promise<APIResponse> {
    return this.write(`/customers/${encodeURIComponent(customerId)}/refund`, body, options);
  }

  /** Sends a body exactly as given, for malformed-JSON and content-type tests. */
  postRaw(path: string, rawBody: string, headers: Record<string, string> = {}): Promise<APIResponse> {
    return this.request.post(path, { data: rawBody, headers: { 'Content-Type': 'application/json', ...headers } });
  }

  private write(path: string, body: unknown, { idempotencyKey }: WriteOptions): Promise<APIResponse> {
    return this.request.post(path, {
      data: body,
      headers: idempotencyKey === undefined ? {} : { 'Idempotency-Key': idempotencyKey },
    });
  }
}

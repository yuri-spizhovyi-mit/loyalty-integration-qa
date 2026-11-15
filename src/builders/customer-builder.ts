import { randomUUID } from 'node:crypto';
import type { CreateCustomerBody } from '../clients/loyalty-types.js';

/** A customer ID no other test (or test run) will use: `qa-<time>-<random>`. */
export function uniqueCustomerId(prefix = 'qa'): string {
  return `${prefix}-${Date.now().toString(36)}-${randomUUID().slice(0, 8)}`;
}

export class CustomerBuilder {
  private body: CreateCustomerBody = { customerId: uniqueCustomerId(), marketingConsent: false };

  withId(customerId: string): this {
    this.body.customerId = customerId;
    return this;
  }

  withMarketingConsent(consent = true): this {
    this.body.marketingConsent = consent;
    return this;
  }

  build(): CreateCustomerBody {
    return { ...this.body };
  }
}

export const aCustomer = () => new CustomerBuilder();

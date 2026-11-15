/**
 * Response shapes as the tests expect them. These are written from the API
 * contract, not imported from the service, so a change in the service shows
 * up as a test failure instead of silently changing the expectation.
 */
export type Tier = 'Bronze' | 'Silver' | 'Gold';
export type EntryType = 'EARN' | 'REDEEM' | 'REFUND';

export interface CustomerResponse {
  customerId: string;
  marketingConsent: boolean;
  balance: number;
  qualifyingPoints: number;
  tier: Tier;
  createdAt: string;
}

export interface TierResponse {
  customerId: string;
  tier: Tier;
  qualifyingPoints: number;
  nextTier: Tier | null;
  pointsToNextTier: number | null;
}

export interface LedgerEntry {
  transactionId: string;
  customerId: string;
  type: EntryType;
  /** Signed: EARN is positive, REDEEM and REFUND are negative. */
  points: number;
  balanceAfter: number;
  reference: string | null;
  refundOf: string | null;
  correlationId: string;
  createdAt: string;
}

export interface LedgerResponse {
  customerId: string;
  balance: number;
  entries: LedgerEntry[];
}

export interface TransactionResponse {
  transaction: LedgerEntry;
  balance: number;
  qualifyingPoints: number;
  tier: Tier;
}

export type EventType =
  | 'customer.created'
  | 'points.earned'
  | 'points.redeemed'
  | 'points.refunded'
  | 'tier.changed';

/** An event as published by the engine. The exact shape per type is in asyncapi.yaml. */
export interface LoyaltyEvent<T = Record<string, unknown>> {
  eventId: string;
  type: EventType;
  sequence: number;
  customerId: string;
  correlationId: string;
  occurredAt: string;
  data: T;
}

export interface TransactionEventData {
  transactionId: string;
  entryType: EntryType;
  points: number;
  balanceAfter: number;
  reference: string | null;
  refundOf: string | null;
}

export interface CustomerCreatedData {
  tier: Tier;
  marketingConsent: boolean;
}

export interface TierChangedData {
  previousTier: Tier;
  newTier: Tier;
  direction: 'upgrade' | 'downgrade';
  qualifyingPoints: number;
  transactionId: string;
}

export interface OutboxResponse {
  customerId: string;
  events: LoyaltyEvent[];
}

export interface ErrorResponse {
  error: {
    code: string;
    message: string;
    details?: { field: string; message: string }[];
  };
  correlationId: string;
}

export interface CreateCustomerBody {
  customerId: string;
  marketingConsent?: boolean;
}

export interface PointsBody {
  points: number;
  reference?: string;
}

export interface RefundBody {
  transactionId: string;
  points?: number;
  reference?: string;
}

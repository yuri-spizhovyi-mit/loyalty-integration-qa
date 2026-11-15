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

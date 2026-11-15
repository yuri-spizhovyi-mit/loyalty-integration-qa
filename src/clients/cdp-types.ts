/** Customer data platform shapes, written from cdp-openapi.yaml. */
export type ProfileTier = 'bronze' | 'silver' | 'gold';

export interface ProfileAttributes {
  loyalty_tier: ProfileTier | null;
  points_balance: number | null;
  marketing_consent: boolean | null;
  member_since: string | null;
  last_transaction_id: string | null;
  last_transaction_type: 'earn' | 'redeem' | 'refund' | null;
  last_activity_at: string | null;
}

export interface ProfileSync {
  lastSequence: number;
  appliedEvents: number;
  duplicateEvents: number;
  outOfOrderEvents: number;
  missingSequences: number[];
  updatedAt: string;
}

export interface Profile {
  profileId: string;
  attributes: ProfileAttributes;
  sync: ProfileSync;
}

export type FaultMode = 'fail-before-apply' | 'fail-after-apply' | 'crash-after-apply';

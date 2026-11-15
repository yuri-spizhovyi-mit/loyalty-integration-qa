import type { LoyaltyEvent } from './events.js';

/**
 * Customer profiles built from loyalty events.
 *
 * Field mapping (source event field -> profile attribute):
 *
 *   customer.created  customerId            -> profileId
 *   customer.created  data.tier "Bronze"    -> attributes.loyalty_tier "bronze"
 *   customer.created  data.marketingConsent -> attributes.marketing_consent
 *   customer.created  occurredAt            -> attributes.member_since
 *   customer.created  (new member)          -> attributes.points_balance 0
 *   tier.changed      data.newTier "Gold"   -> attributes.loyalty_tier "gold"
 *   points.*          data.balanceAfter     -> attributes.points_balance
 *   points.*          data.transactionId    -> attributes.last_transaction_id
 *   points.*          data.entryType "EARN" -> attributes.last_transaction_type "earn"
 *   any event         occurredAt            -> attributes.last_activity_at
 *
 * Reliability rules:
 * - Duplicates (same eventId) are acknowledged and ignored.
 * - Each group of attributes remembers the sequence of the event that last
 *   wrote it, and an older event never overwrites a newer one. Out-of-order
 *   delivery therefore converges to the same profile as in-order delivery.
 * - Sequences not seen yet below the highest one are reported as missing.
 */

export interface ProfileAttributes {
  loyalty_tier: 'bronze' | 'silver' | 'gold' | null;
  points_balance: number | null;
  marketing_consent: boolean | null;
  member_since: string | null;
  last_transaction_id: string | null;
  last_transaction_type: 'earn' | 'redeem' | 'refund' | null;
  last_activity_at: string | null;
}

export interface Profile {
  profileId: string;
  attributes: ProfileAttributes;
  sync: {
    lastSequence: number;
    appliedEvents: number;
    duplicateEvents: number;
    outOfOrderEvents: number;
    missingSequences: number[];
    updatedAt: string;
  };
}

type FieldGroup = 'tier' | 'balance' | 'membership' | 'activity';

interface ProfileState {
  profile: Profile;
  versions: Record<FieldGroup, number>;
  sequences: Set<number>;
}

export type ApplyResult = 'applied' | 'duplicate';

const MAX_MISSING_REPORTED = 100;

export class ProfileStore {
  private readonly states = new Map<string, ProfileState>();
  private readonly seenEventIds = new Set<string>();

  get(profileId: string): Profile | undefined {
    const state = this.states.get(profileId);
    return state ? structuredClone(state.profile) : undefined;
  }

  apply(event: LoyaltyEvent): ApplyResult {
    const state = this.stateFor(event.customerId);
    const { profile } = state;
    const now = new Date().toISOString();

    if (this.seenEventIds.has(event.eventId)) {
      profile.sync.duplicateEvents += 1;
      profile.sync.updatedAt = now;
      return 'duplicate';
    }
    this.seenEventIds.add(event.eventId);

    if (event.sequence < profile.sync.lastSequence) profile.sync.outOfOrderEvents += 1;
    state.sequences.add(event.sequence);
    profile.sync.lastSequence = Math.max(profile.sync.lastSequence, event.sequence);
    profile.sync.appliedEvents += 1;
    profile.sync.missingSequences = this.missing(state);
    profile.sync.updatedAt = now;

    const set = (group: FieldGroup, values: Partial<ProfileAttributes>) => {
      if (event.sequence <= state.versions[group]) return; // an equal or newer event already wrote these
      state.versions[group] = event.sequence;
      Object.assign(profile.attributes, values);
    };

    switch (event.type) {
      case 'customer.created':
        set('membership', { marketing_consent: event.data.marketingConsent, member_since: event.occurredAt });
        set('tier', { loyalty_tier: lowerTier(event.data.tier) });
        set('balance', { points_balance: 0 });
        break;
      case 'tier.changed':
        set('tier', { loyalty_tier: lowerTier(event.data.newTier) });
        break;
      case 'points.earned':
      case 'points.redeemed':
      case 'points.refunded':
        set('balance', {
          points_balance: event.data.balanceAfter,
          last_transaction_id: event.data.transactionId,
          last_transaction_type: event.data.entryType.toLowerCase() as ProfileAttributes['last_transaction_type'],
        });
        break;
    }
    set('activity', { last_activity_at: event.occurredAt });
    return 'applied';
  }

  private stateFor(profileId: string): ProfileState {
    let state = this.states.get(profileId);
    if (!state) {
      state = {
        profile: {
          profileId,
          attributes: {
            loyalty_tier: null,
            points_balance: null,
            marketing_consent: null,
            member_since: null,
            last_transaction_id: null,
            last_transaction_type: null,
            last_activity_at: null,
          },
          sync: {
            lastSequence: 0,
            appliedEvents: 0,
            duplicateEvents: 0,
            outOfOrderEvents: 0,
            missingSequences: [],
            updatedAt: new Date().toISOString(),
          },
        },
        versions: { tier: 0, balance: 0, membership: 0, activity: 0 },
        sequences: new Set(),
      };
      this.states.set(profileId, state);
    }
    return state;
  }

  private missing(state: ProfileState): number[] {
    const result: number[] = [];
    for (let seq = 1; seq < state.profile.sync.lastSequence && result.length < MAX_MISSING_REPORTED; seq += 1) {
      if (!state.sequences.has(seq)) result.push(seq);
    }
    return result;
  }
}

function lowerTier(tier: string): ProfileAttributes['loyalty_tier'] {
  return tier.toLowerCase() as ProfileAttributes['loyalty_tier'];
}

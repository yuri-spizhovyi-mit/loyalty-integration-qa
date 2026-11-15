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

/** Everything the platform keeps per customer: the profile plus what it needs to merge events. */
export interface ProfileState {
  profile: Profile;
  versions: Record<FieldGroup, number>;
  sequences: Set<number>;
}

export type ApplyResult = 'applied' | 'duplicate';

export interface ApplyOutcome {
  result: ApplyResult;
  /**
   * Set when a tier.changed event wrote the profile's tier. Not set for a
   * duplicate, or for a stale tier.changed that a newer event already
   * overrode, so downstream systems only hear about tier changes that the
   * profile actually shows.
   */
  tierChange: { marketingConsent: boolean | null } | null;
}

/**
 * Where profiles live. Two implementations with the same rules: in memory
 * (ProfileStore, the default) and SQLite (SqliteProfileStore, used on Kafka
 * so a restarted or second instance sees the same state).
 */
export interface ProfileRepository {
  get(profileId: string): Profile | undefined;
  apply(event: LoyaltyEvent): ApplyOutcome;
  /** Runs `work` atomically: either all its writes happen, or none. */
  transaction<T>(work: () => T): T;
}

const MAX_MISSING_REPORTED = 100;

export function newProfileState(profileId: string): ProfileState {
  return {
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
}

/** Counts a duplicate delivery on the profile. */
export function recordDuplicate(state: ProfileState): ApplyOutcome {
  state.profile.sync.duplicateEvents += 1;
  state.profile.sync.updatedAt = new Date().toISOString();
  return { result: 'duplicate', tierChange: null };
}

/** Applies a new (not yet seen) event to the state, with the versioning rules above. */
export function applyNewEvent(state: ProfileState, event: LoyaltyEvent): ApplyOutcome {
  const { profile } = state;
  if (event.sequence < profile.sync.lastSequence) profile.sync.outOfOrderEvents += 1;
  state.sequences.add(event.sequence);
  profile.sync.lastSequence = Math.max(profile.sync.lastSequence, event.sequence);
  profile.sync.appliedEvents += 1;
  profile.sync.missingSequences = missing(state);
  profile.sync.updatedAt = new Date().toISOString();

  const set = (group: FieldGroup, values: Partial<ProfileAttributes>): boolean => {
    if (event.sequence <= state.versions[group]) return false; // an equal or newer event already wrote these
    state.versions[group] = event.sequence;
    Object.assign(profile.attributes, values);
    return true;
  };

  let tierChange: ApplyOutcome['tierChange'] = null;
  switch (event.type) {
    case 'customer.created':
      set('membership', { marketing_consent: event.data.marketingConsent, member_since: event.occurredAt });
      set('tier', { loyalty_tier: lowerTier(event.data.tier) });
      set('balance', { points_balance: 0 });
      break;
    case 'tier.changed':
      if (set('tier', { loyalty_tier: lowerTier(event.data.newTier) })) {
        tierChange = { marketingConsent: profile.attributes.marketing_consent };
      }
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
  return { result: 'applied', tierChange };
}

function missing(state: ProfileState): number[] {
  const result: number[] = [];
  for (let seq = 1; seq < state.profile.sync.lastSequence && result.length < MAX_MISSING_REPORTED; seq += 1) {
    if (!state.sequences.has(seq)) result.push(seq);
  }
  return result;
}

/** In-memory profiles: the default, and what the in-memory broker setup uses. */
export class ProfileStore implements ProfileRepository {
  private readonly states = new Map<string, ProfileState>();
  private readonly seenEventIds = new Set<string>();

  get(profileId: string): Profile | undefined {
    const state = this.states.get(profileId);
    return state ? structuredClone(state.profile) : undefined;
  }

  apply(event: LoyaltyEvent): ApplyOutcome {
    let state = this.states.get(event.customerId);
    if (!state) {
      state = newProfileState(event.customerId);
      this.states.set(event.customerId, state);
    }
    if (this.seenEventIds.has(event.eventId)) return recordDuplicate(state);
    this.seenEventIds.add(event.eventId);
    return applyNewEvent(state, event);
  }

  /** Single-threaded and in memory: nothing can interleave, so this just runs the work. */
  transaction<T>(work: () => T): T {
    return work();
  }
}

function lowerTier(tier: string): ProfileAttributes['loyalty_tier'] {
  return tier.toLowerCase() as ProfileAttributes['loyalty_tier'];
}

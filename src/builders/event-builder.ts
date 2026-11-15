import { randomUUID } from 'node:crypto';
import type {
  CustomerCreatedData,
  LoyaltyEvent,
  Tier,
  TierChangedData,
  TransactionEventData,
} from '../clients/loyalty-types.js';

const TIER_ORDER: Tier[] = ['Bronze', 'Silver', 'Gold'];

function transactionId(): string {
  return `txn_${randomUUID()}`;
}

/**
 * Builds a valid, consistent event stream for one synthetic customer, the way
 * the engine would publish it: sequence numbers count up from 1 and
 * `balanceAfter` follows the transactions. Tests then publish the events
 * straight to the broker in any order, twice, or with fields broken, to test
 * the consumer without going through the engine.
 *
 *   const stream = anEventStream(customerId, correlationId);
 *   const created = stream.customerCreated();
 *   const earned = stream.earned(1200);
 *   const upgrade = stream.tierChanged('Bronze', 'Silver', 1200, earned);
 */
export class EventStream {
  private sequence = 0;
  private balance = 0;
  private clock = Date.now();

  constructor(
    readonly customerId: string,
    private readonly correlationId: string,
  ) {}

  customerCreated(marketingConsent = false): LoyaltyEvent<CustomerCreatedData> {
    return this.next('customer.created', { tier: 'Bronze', marketingConsent });
  }

  earned(points: number): LoyaltyEvent<TransactionEventData> {
    return this.transaction('points.earned', 'EARN', points, null);
  }

  redeemed(points: number): LoyaltyEvent<TransactionEventData> {
    return this.transaction('points.redeemed', 'REDEEM', -points, null);
  }

  refunded(points: number, of: LoyaltyEvent<TransactionEventData>): LoyaltyEvent<TransactionEventData> {
    return this.transaction('points.refunded', 'REFUND', -points, of.data.transactionId);
  }

  tierChanged(
    previousTier: Tier,
    newTier: Tier,
    qualifyingPoints: number,
    cause: LoyaltyEvent<TransactionEventData>,
  ): LoyaltyEvent<TierChangedData> {
    const direction = TIER_ORDER.indexOf(newTier) > TIER_ORDER.indexOf(previousTier) ? 'upgrade' : 'downgrade';
    return this.next('tier.changed', {
      previousTier,
      newTier,
      direction,
      qualifyingPoints,
      transactionId: cause.data.transactionId,
    });
  }

  private transaction(
    type: 'points.earned' | 'points.redeemed' | 'points.refunded',
    entryType: TransactionEventData['entryType'],
    points: number,
    refundOf: string | null,
  ): LoyaltyEvent<TransactionEventData> {
    this.balance += points;
    return this.next(type, {
      transactionId: transactionId(),
      entryType,
      points,
      balanceAfter: this.balance,
      reference: `order-${randomUUID().slice(0, 8)}`,
      refundOf,
    });
  }

  private next<T>(type: LoyaltyEvent['type'], data: T): LoyaltyEvent<T> {
    this.sequence += 1;
    this.clock += 1000; // one second apart, so occurredAt also orders the stream
    return {
      eventId: randomUUID(),
      type,
      sequence: this.sequence,
      customerId: this.customerId,
      correlationId: this.correlationId,
      occurredAt: new Date(this.clock).toISOString(),
      data,
    };
  }
}

export const anEventStream = (customerId: string, correlationId: string) => new EventStream(customerId, correlationId);

import { setTimeout as sleep } from 'node:timers/promises';
import { KeyedMutex } from './concurrency.js';
import { type LedgerEntry, LoyaltyStore } from './domain/store.js';
import { nextTierFor, type TierName, tierFor, tierRank } from './domain/tiers.js';
import { ApiError } from './errors.js';
import { buildEvent, type EventPublisher, type LoyaltyEventType } from './events.js';
import type { CreateCustomerRequest, PointsRequest, RefundRequest } from './validation.js';

export interface ServiceOptions {
  /**
   * Simulated write latency between reading the balance and appending the
   * entry. It widens the race window so the concurrency tests mean something.
   */
  processingDelayMs: number;
}

export interface TransactionResult {
  transaction: LedgerEntry;
  balance: number;
  qualifyingPoints: number;
  tier: TierName;
}

export class LoyaltyService {
  private readonly locks = new KeyedMutex();

  constructor(
    private readonly store: LoyaltyStore,
    private readonly publisher: EventPublisher,
    private readonly options: ServiceOptions,
  ) {}

  createCustomer(request: CreateCustomerRequest) {
    if (this.store.getCustomer(request.customerId)) {
      throw new ApiError('CUSTOMER_ALREADY_EXISTS', `Customer ${request.customerId} already exists`);
    }
    this.store.addCustomer({ ...request, createdAt: new Date().toISOString() });
    return this.customerSummary(request.customerId);
  }

  customerSummary(customerId: string) {
    const customer = this.requireCustomer(customerId);
    const qualifyingPoints = this.store.qualifyingPoints(customerId);
    return {
      customerId,
      marketingConsent: customer.marketingConsent,
      balance: this.store.balance(customerId),
      qualifyingPoints,
      tier: tierFor(qualifyingPoints),
      createdAt: customer.createdAt,
    };
  }

  tier(customerId: string) {
    this.requireCustomer(customerId);
    const qualifyingPoints = this.store.qualifyingPoints(customerId);
    const next = nextTierFor(qualifyingPoints);
    return {
      customerId,
      tier: tierFor(qualifyingPoints),
      qualifyingPoints,
      nextTier: next?.name ?? null,
      pointsToNextTier: next?.pointsNeeded ?? null,
    };
  }

  ledger(customerId: string) {
    this.requireCustomer(customerId);
    return {
      customerId,
      balance: this.store.balance(customerId),
      entries: this.store.entries(customerId),
    };
  }

  earn(customerId: string, request: PointsRequest, correlationId: string): Promise<TransactionResult> {
    return this.transact(customerId, correlationId, 'points.earned', () => ({
      type: 'EARN',
      points: request.points,
      reference: request.reference,
      refundOf: null,
    }));
  }

  redeem(customerId: string, request: PointsRequest, correlationId: string): Promise<TransactionResult> {
    return this.transact(customerId, correlationId, 'points.redeemed', () => {
      const balance = this.store.balance(customerId);
      if (request.points > balance) {
        throw new ApiError(
          'INSUFFICIENT_BALANCE',
          `Cannot redeem ${request.points} points; balance is ${balance}`,
        );
      }
      return { type: 'REDEEM', points: -request.points, reference: request.reference, refundOf: null };
    });
  }

  refund(customerId: string, request: RefundRequest, correlationId: string): Promise<TransactionResult> {
    return this.transact(customerId, correlationId, 'points.refunded', () => {
      const original = this.store.getEntry(request.transactionId);
      if (!original || original.customerId !== customerId) {
        throw new ApiError(
          'TRANSACTION_NOT_FOUND',
          `Transaction ${request.transactionId} not found for customer ${customerId}`,
        );
      }
      if (original.type !== 'EARN') {
        throw new ApiError('REFUND_NOT_ALLOWED', `Only EARN transactions can be refunded, not ${original.type}`);
      }
      const refundable = original.points - this.store.refundedAgainst(original.transactionId);
      const points = request.points ?? refundable;
      if (points < 1 || points > refundable) {
        throw new ApiError(
          'REFUND_EXCEEDS_ORIGINAL',
          `Cannot refund ${points} points; ${refundable} of ${original.points} are refundable`,
        );
      }
      const balance = this.store.balance(customerId);
      if (points > balance) {
        throw new ApiError(
          'INSUFFICIENT_BALANCE',
          `Cannot refund ${points} points; balance is ${balance} (points were already redeemed)`,
        );
      }
      return { type: 'REFUND', points: -points, reference: request.reference, refundOf: original.transactionId };
    });
  }

  /**
   * One ledger write: lock the customer, check the rules, append the entry,
   * then publish the transaction event and, if the tier moved, tier.changed.
   */
  private async transact(
    customerId: string,
    correlationId: string,
    eventType: LoyaltyEventType,
    decide: () => Pick<LedgerEntry, 'type' | 'points' | 'reference' | 'refundOf'>,
  ): Promise<TransactionResult> {
    this.requireCustomer(customerId);
    return this.locks.run(customerId, async () => {
      const tierBefore = tierFor(this.store.qualifyingPoints(customerId));
      const change = decide();
      if (this.options.processingDelayMs > 0) await sleep(this.options.processingDelayMs);

      const entry = this.store.append({ ...change, customerId, correlationId });
      const qualifyingPoints = this.store.qualifyingPoints(customerId);
      const tierAfter = tierFor(qualifyingPoints);

      this.publish(eventType, customerId, correlationId, {
        transactionId: entry.transactionId,
        entryType: entry.type,
        points: entry.points,
        balanceAfter: entry.balanceAfter,
        reference: entry.reference,
        refundOf: entry.refundOf,
      });
      if (tierAfter !== tierBefore) {
        this.publish('tier.changed', customerId, correlationId, {
          previousTier: tierBefore,
          newTier: tierAfter,
          direction: tierRank(tierAfter) > tierRank(tierBefore) ? 'upgrade' : 'downgrade',
          qualifyingPoints,
          transactionId: entry.transactionId,
        });
      }

      return { transaction: entry, balance: entry.balanceAfter, qualifyingPoints, tier: tierAfter };
    });
  }

  private publish(type: LoyaltyEventType, customerId: string, correlationId: string, data: Record<string, unknown>) {
    this.publisher.publish(
      buildEvent({ type, customerId, correlationId, sequence: this.store.nextSequence(customerId), data }),
    );
  }

  private requireCustomer(customerId: string) {
    const customer = this.store.getCustomer(customerId);
    if (!customer) throw new ApiError('CUSTOMER_NOT_FOUND', `Customer ${customerId} not found`);
    return customer;
  }
}

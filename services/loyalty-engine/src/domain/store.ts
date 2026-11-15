import { randomUUID } from 'node:crypto';

export type EntryType = 'EARN' | 'REDEEM' | 'REFUND';

export interface Customer {
  customerId: string;
  marketingConsent: boolean;
  createdAt: string;
}

/**
 * One row in the append-only points ledger. `points` is signed:
 * EARN is positive, REDEEM and REFUND are negative, so the balance is
 * always the plain sum of a customer's entries.
 */
export interface LedgerEntry {
  transactionId: string;
  customerId: string;
  type: EntryType;
  points: number;
  balanceAfter: number;
  reference: string | null;
  refundOf: string | null;
  correlationId: string;
  createdAt: string;
}

export class LoyaltyStore {
  private readonly customers = new Map<string, Customer>();
  private readonly ledgers = new Map<string, LedgerEntry[]>();
  private readonly entriesById = new Map<string, LedgerEntry>();
  private readonly sequences = new Map<string, number>();

  addCustomer(customer: Customer): void {
    this.customers.set(customer.customerId, customer);
    this.ledgers.set(customer.customerId, []);
    this.sequences.set(customer.customerId, 0);
  }

  getCustomer(customerId: string): Customer | undefined {
    return this.customers.get(customerId);
  }

  entries(customerId: string): readonly LedgerEntry[] {
    return this.ledgers.get(customerId) ?? [];
  }

  getEntry(transactionId: string): LedgerEntry | undefined {
    return this.entriesById.get(transactionId);
  }

  balance(customerId: string): number {
    return this.entries(customerId).reduce((sum, entry) => sum + entry.points, 0);
  }

  /** Earned minus refunded. Redemptions do not count against the tier. */
  qualifyingPoints(customerId: string): number {
    return this.entries(customerId)
      .filter((entry) => entry.type !== 'REDEEM')
      .reduce((sum, entry) => sum + entry.points, 0);
  }

  /** Points already refunded against one EARN entry (a positive number). */
  refundedAgainst(earnTransactionId: string): number {
    const earn = this.entriesById.get(earnTransactionId);
    if (!earn) return 0;
    return -this.entries(earn.customerId)
      .filter((entry) => entry.refundOf === earnTransactionId)
      .reduce((sum, entry) => sum + entry.points, 0);
  }

  append(entry: Omit<LedgerEntry, 'transactionId' | 'balanceAfter' | 'createdAt'>): LedgerEntry {
    const ledger = this.ledgers.get(entry.customerId);
    if (!ledger) throw new Error(`No ledger for customer ${entry.customerId}`);
    const full: LedgerEntry = {
      ...entry,
      transactionId: `txn_${randomUUID()}`,
      balanceAfter: this.balance(entry.customerId) + entry.points,
      createdAt: new Date().toISOString(),
    };
    ledger.push(full);
    this.entriesById.set(full.transactionId, full);
    return full;
  }

  /** Per-customer event sequence number, so consumers can spot gaps and reordering. */
  nextSequence(customerId: string): number {
    const next = (this.sequences.get(customerId) ?? 0) + 1;
    this.sequences.set(customerId, next);
    return next;
  }
}

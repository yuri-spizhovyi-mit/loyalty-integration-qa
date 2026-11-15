/**
 * Test-only fault injection, scoped to one customer so parallel tests do not
 * affect each other.
 *
 * - fail-before-apply: answer 503 without touching the profile (a crash or a
 *   database outage). The broker retries.
 * - fail-after-apply: apply the event, then answer 503 (the ack was lost).
 *   The broker redelivers an event that was already applied, which the
 *   consumer must recognise as a duplicate.
 * - crash-after-apply: apply the event (and commit it to the store), then
 *   kill the process before answering. Only meaningful on Kafka with the
 *   SQLite store: the container restarts, the offset was never committed,
 *   and the event is delivered again.
 */
export type FaultMode = 'fail-before-apply' | 'fail-after-apply' | 'crash-after-apply';

export interface Fault {
  mode: FaultMode;
  /** How many more deliveries to fail. */
  remaining: number;
}

export class FaultInjector {
  private readonly faults = new Map<string, Fault>();

  set(customerId: string, fault: Fault): void {
    this.faults.set(customerId, { ...fault });
  }

  clear(customerId: string): void {
    this.faults.delete(customerId);
  }

  /** Returns the fault mode to apply to this delivery, if any, and counts it down. */
  take(customerId: string): FaultMode | null {
    const fault = this.faults.get(customerId);
    if (!fault || fault.remaining <= 0) return null;
    fault.remaining -= 1;
    return fault.mode;
  }
}

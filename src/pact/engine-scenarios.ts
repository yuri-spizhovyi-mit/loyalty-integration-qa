import { randomUUID } from 'node:crypto';
import { LoyaltyStore } from '../../services/loyalty-engine/src/domain/store.js';
import { InMemoryEventLog, type LoyaltyEvent } from '../../services/loyalty-engine/src/events.js';
import { LoyaltyService } from '../../services/loyalty-engine/src/service.js';

/**
 * Runs a scenario on a fresh, in-process loyalty engine (its real service
 * code) and returns every event it published, in order. Provider
 * verifications use it so their messages come from the producer's code,
 * not from hand-written copies of it.
 */
export async function engineEvents(
  scenario: (engine: LoyaltyService, customerId: string, correlationId: string) => Promise<unknown>,
  options: { marketingConsent?: boolean } = {},
): Promise<LoyaltyEvent[]> {
  const log = new InMemoryEventLog();
  const engine = new LoyaltyService(new LoyaltyStore(), log, { processingDelayMs: 0 });
  const customerId = `pact-${randomUUID().slice(0, 8)}`;
  const correlationId = `corr-${customerId}`;
  engine.createCustomer({ customerId, marketingConsent: options.marketingConsent ?? true }, correlationId);
  await scenario(engine, customerId, correlationId);
  return log.forCustomer(customerId);
}

export function lastOfType(events: LoyaltyEvent[], type: LoyaltyEvent['type']): LoyaltyEvent {
  const event = events.filter((e) => e.type === type).at(-1);
  if (!event) throw new Error(`the scenario published no ${type} event`);
  return event;
}

export const earn = (engine: LoyaltyService, customerId: string, correlationId: string, points: number) =>
  engine.earn(customerId, { points, reference: 'order-1001' }, correlationId);

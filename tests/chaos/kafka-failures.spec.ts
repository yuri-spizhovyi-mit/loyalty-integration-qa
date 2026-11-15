import { aCustomer } from '../../src/builders/customer-builder.js';
import { anEarn } from '../../src/builders/transaction-builder.js';
import type { CdpClient } from '../../src/clients/cdp-client.js';
import type { LoyaltyClient } from '../../src/clients/loyalty-client.js';
import type { CustomerResponse, OutboxResponse } from '../../src/clients/loyalty-types.js';

interface RelayStatus {
  transport: string;
  pending: number;
  failedSends: number;
  lastError: string | null;
}
import { config } from '../../src/config.js';
import { expect, test } from '../../src/fixtures.js';
import { compose, RECOVERY_POLL, restartCount, waitHealthy } from '../../src/helpers/chaos.js';
import { deliverInOrder } from '../../src/helpers/delivery.js';
import { readJson } from '../../src/helpers/json.js';
import { PARTITIONS } from '../../src/helpers/kafka-placement.js';
import { waitForDecisions } from '../../src/helpers/notification-wait.js';

/**
 * Kafka failure modes, driven through Docker Compose: a consumer that
 * crashes mid-record, Kafka itself going away, and a consumer group that
 * grows and shrinks. In every case the end state must be the same as with
 * no failure: nothing lost, nothing applied twice.
 *
 * These need the CDP's SQLite store (docker-compose.kafka.yml), so state
 * survives a restart and is shared between instances.
 */

/** Waits until the profile has applied every event the engine published, with a recovery-sized timeout. */
async function waitForFullSync(loyalty: LoyaltyClient, cdp: CdpClient, customerId: string) {
  await expect
    .poll(
      async () => {
        const { events } = await readJson<OutboxResponse>(await loyalty.getOutbox(customerId));
        const profile = await cdp.profileOrNull(customerId).catch(() => null);
        return profile ? `${profile.sync.appliedEvents}/${events.length}` : `none/${events.length}`;
      },
      { ...RECOVERY_POLL, message: `profile ${customerId}: applied/published` },
    )
    .toMatch(/^(\d+)\/\1$/);
  return (await cdp.profileOrNull(customerId))!;
}

interface ConsumerInfo {
  ready: boolean;
  assignedPartitions: number[];
  streamRestarts: number;
}

/**
 * How long Kafka stays down in the outage test: longer than the consumers'
 * 10 s session timeout, so their group memberships expire and must be
 * rebuilt. This is the scenario's length, not a wait for something to happen.
 */
const OUTAGE_MS = 15_000;

async function consumerInfo(baseUrl: string): Promise<ConsumerInfo | null> {
  try {
    const response = await fetch(`${baseUrl}/internal/consumer`, { signal: AbortSignal.timeout(1000) });
    return response.ok ? ((await response.json()) as ConsumerInfo) : null;
  } catch {
    return null;
  }
}

test.describe('Chaos: a consumer crashes mid-record', () => {
  test('the CDP dies after applying an event but before committing; after the restart the event is a duplicate', async ({
    queues,
    cdp,
    notifications,
    eventStream,
  }) => {
    const created = eventStream.customerCreated(true);
    const earned = eventStream.earned(1000);
    await deliverInOrder(queues, [created, earned]);
    const restartsBefore = restartCount('customer-data-platform');

    // Applied and stored (profile + outbox row in one transaction), then the process exits.
    await cdp.setFault(eventStream.customerId, 'crash-after-apply', 1);
    const upgrade = eventStream.tierChanged('Bronze', 'Silver', 1000, earned);
    const messageId = await queues.publish(upgrade);

    await expect.poll(() => restartCount('customer-data-platform'), { ...RECOVERY_POLL, message: 'Docker restarted the CDP' }).toBeGreaterThan(restartsBefore);
    await waitHealthy(config.cdpUrl);

    // The offset was never committed, so the restarted consumer reads the record again.
    await expect
      .poll(async () => (await queues.delivery(messageId)).status, { ...RECOVERY_POLL, message: 'redelivered after restart' })
      .toBe('delivered');
    const profile = await cdp.profileOrNull(eventStream.customerId);
    expect(profile?.attributes.loyalty_tier).toBe('silver');
    expect(profile?.sync).toMatchObject({ appliedEvents: 3, duplicateEvents: 1 });

    // The profile event was in the persisted outbox: sent after the restart, once.
    const result = await waitForDecisions(notifications, eventStream.customerId, [upgrade.eventId]);
    expect(result.notifications).toHaveLength(1);
    expect(result.decisions.map((d) => d.outcome)).toEqual(['sent']);
  });
});

test.describe('Chaos: Kafka goes away', () => {
  test('writes during a Kafka outage succeed, and their events arrive once Kafka is back', async ({
    loyalty,
    cdp,
    createCustomer,
  }) => {
    const { customerId } = await createCustomer(aCustomer().withMarketingConsent(true).build());
    await waitForFullSync(loyalty, cdp, customerId);

    const relay = async () => readJson<RelayStatus>(await loyalty.getRelayStatus());

    compose('stop', 'kafka');
    const stoppedAt = Date.now();
    try {
      // The engine's ledger does not depend on Kafka; its relay keeps the events and retries.
      for (const points of [300, 400, 500]) {
        await expect(await loyalty.earn(customerId, anEarn().points(points).build())).toHaveStatus(201);
      }
      // The relay holds the events: the Kafka client waits through the outage
      // instead of failing the send, so nothing is dropped and nothing errors.
      await expect
        .poll(async () => (await relay()).pending, { ...RECOVERY_POLL, message: 'relay holds the events while Kafka is down' })
        .toBeGreaterThanOrEqual(3);
      const rest = OUTAGE_MS - (Date.now() - stoppedAt);
      if (rest > 0) await new Promise((resolve) => setTimeout(resolve, rest));
    } finally {
      compose('start', 'kafka');
    }
    await expect.poll(async () => (await relay()).pending, { ...RECOVERY_POLL, message: 'relay drained' }).toBe(0);
    await waitHealthy(config.cdpUrl);

    const profile = await waitForFullSync(loyalty, cdp, customerId);
    const engine = await readJson<CustomerResponse>(await loyalty.getCustomer(customerId));
    expect(profile.attributes).toMatchObject({ points_balance: engine.balance, loyalty_tier: 'silver' });
    expect(engine.balance).toBe(1200);
    expect(profile.sync.missingSequences).toEqual([]);
  });
});

test.describe('Chaos: a consumer stream breaks', () => {
  test('the supervisor restarts a broken stream, and the consumer keeps reading', async ({ queues, cdp, eventStream }) => {
    // A client library can give up and close the stream (it did after a long
    // outage). Without a supervisor the consumer stays in its group, owns no
    // partitions and reads nothing, while looking healthy.
    const before = (await consumerInfo(config.cdpUrl))!.streamRestarts;
    const response = await fetch(`${config.cdpUrl}/internal/consumer/stream-failure`, { method: 'POST' });
    expect(response.status).toBe(202);

    await expect
      .poll(async () => (await consumerInfo(config.cdpUrl))?.streamRestarts ?? before, { ...RECOVERY_POLL, message: 'stream restarted' })
      .toBeGreaterThan(before);
    await waitHealthy(config.cdpUrl);

    // And it really reads again: a new event reaches the profile.
    await deliverInOrder(queues, [eventStream.customerCreated(true)]);
    expect((await cdp.profileOrNull(eventStream.customerId))?.attributes.loyalty_tier).toBe('bronze');
  });
});

test.describe('Chaos: the consumer group grows and shrinks', () => {
  test('a second CDP instance takes over partitions and gives them back; nothing is lost or applied twice', async ({
    loyalty,
    cdp,
    createCustomer,
  }) => {
    const customers = await Promise.all(Array.from({ length: 9 }, () => createCustomer()));
    const earnForAll = (points: number) =>
      Promise.all(customers.map(({ customerId }) => loyalty.earn(customerId, anEarn().points(points).build())));

    // --no-deps: Kafka and the topics already exist; do not re-run kafka-init.
    compose('--profile', 'scale', 'up', '--detach', '--wait', '--no-deps', 'customer-data-platform-2');
    try {
      // Both instances own part of the topic: 3 partitions over 2 members.
      const split = async () => {
        const [one, two] = await Promise.all([consumerInfo(config.cdpUrl), consumerInfo(config.cdp2Url)]);
        const a = one?.assignedPartitions ?? [];
        const b = two?.assignedPartitions ?? [];
        return a.length > 0 && b.length > 0 ? [...a, ...b].sort().join(',') : `cdp-1 [${a}] cdp-2 [${b}]`;
      };
      await expect.poll(split, { ...RECOVERY_POLL, message: 'partitions split between the two instances' }).toBe('0,1,2');

      for (const response of await earnForAll(100)) await expect(response).toHaveStatus(201);
    } finally {
      compose('--profile', 'scale', 'stop', 'customer-data-platform-2');
    }

    // Back to one instance: it owns every partition again.
    await expect
      .poll(async () => (await consumerInfo(config.cdpUrl))?.assignedPartitions, { ...RECOVERY_POLL, message: 'cdp-1 owns all partitions' })
      .toEqual(Array.from({ length: PARTITIONS }, (_, i) => i));
    for (const response of await earnForAll(50)) await expect(response).toHaveStatus(201);

    // Every customer: all events applied exactly once, whichever instance read them.
    for (const { customerId } of customers) {
      const profile = await waitForFullSync(loyalty, cdp, customerId);
      expect(profile.attributes.points_balance, customerId).toBe(150);
      expect(profile.sync.appliedEvents, customerId).toBe(3);
    }
  });
});

test.afterAll(() => {
  // Leave the stack as the other projects expect it.
  compose('--profile', 'scale', 'rm', '--stop', '--force', 'customer-data-platform-2');
  compose('start', 'kafka');
});

import { uniqueCustomerId } from '../builders/customer-builder.js';
import { anEventStream, type EventStream } from '../builders/event-builder.js';
import { waitForDelivery } from './delivery.js';
import { expect } from './matchers.js';
import type { QueueInspector } from './queue-inspector.js';

/** Partitions per topic, as created by docker-compose.kafka.yml. */
export const PARTITIONS = 3;

/** Creates synthetic customers until it has one on each wanted partition, keyed by role. */
export async function customersOnPartitions(
  queues: QueueInspector,
  correlationId: string,
  wanted: { role: string; samePartitionAs?: string }[],
): Promise<Record<string, { stream: EventStream; partition: number }>> {
  const found: Record<string, { stream: EventStream; partition: number }> = {};
  for (let attempt = 0; attempt < 60 && Object.keys(found).length < wanted.length; attempt += 1) {
    const stream = anEventStream(uniqueCustomerId('kfk'), correlationId);
    const created = stream.customerCreated(true);
    const placement = await queues.publishWithPlacement(created);
    await waitForDelivery(queues, placement.messageId, 'delivered');
    const partition = placement.partition!;
    for (const { role, samePartitionAs } of wanted) {
      if (found[role]) continue;
      const anchor = samePartitionAs ? found[samePartitionAs] : undefined;
      const fits = samePartitionAs
        ? anchor !== undefined && anchor.partition === partition
        : Object.values(found).every((f) => f.partition !== partition);
      if (fits) {
        found[role] = { stream, partition };
        break;
      }
    }
  }
  expect(Object.keys(found).sort(), 'could not place customers on the partitions needed').toEqual(wanted.map((w) => w.role).sort());
  return found;
}

import { createApp } from './app.js';
import { Broker, type QueueBinding } from './broker.js';

const port = Number(process.env.PORT ?? 3004);
const cdpUrl = process.env.CDP_URL ?? 'http://localhost:3002';

// One topic today; the notification service gets its own queue in step 5.
const bindings: QueueBinding[] = [
  { queue: 'cdp.loyalty-events', topic: 'loyalty.events', subscriberUrl: `${cdpUrl}/events` },
];

const broker = new Broker(bindings, {
  maxAttempts: Number(process.env.BROKER_MAX_ATTEMPTS ?? 4),
  retryBaseMs: Number(process.env.BROKER_RETRY_BASE_MS ?? 100),
  deliveryTimeoutMs: Number(process.env.BROKER_DELIVERY_TIMEOUT_MS ?? 2000),
});

const server = createApp(broker).listen(port, () => {
  console.log(`[broker] listening on http://localhost:${port}`);
  for (const b of bindings) console.log(`[broker]   ${b.topic} -> ${b.queue} -> ${b.subscriberUrl}`);
});

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => server.close(() => process.exit(0)));
}

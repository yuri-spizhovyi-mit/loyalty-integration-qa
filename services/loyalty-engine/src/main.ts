import { createApp } from './app.js';
import { LoyaltyStore } from './domain/store.js';
import { BrokerRelay, type EventPublisher, FanOutPublisher, InMemoryEventLog } from './events.js';
import { LoyaltyService } from './service.js';

const port = Number(process.env.PORT ?? 3001);
// Simulated write latency. It must be longer than the spread between parallel
// requests arriving (about 10-15 ms locally), or the concurrency tests can pass
// against a broken implementation just because the requests did not overlap.
const processingDelayMs = Number(process.env.LOYALTY_PROCESSING_DELAY_MS ?? 50);
// Without a broker the engine still records events in its outbox; the REST
// and contract suites do not need the rest of the stack.
const brokerUrl = process.env.BROKER_URL;

const outbox = new InMemoryEventLog();
const publisher: EventPublisher = brokerUrl ? new FanOutPublisher(outbox, new BrokerRelay(brokerUrl)) : outbox;
const service = new LoyaltyService(new LoyaltyStore(), publisher, { processingDelayMs });
const server = createApp(service, outbox).listen(port, () => {
  console.log(
    `[loyalty-engine] listening on http://localhost:${port} (processing delay ${processingDelayMs} ms, ` +
      `broker ${brokerUrl ?? 'none'})`,
  );
});

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => server.close(() => process.exit(0)));
}

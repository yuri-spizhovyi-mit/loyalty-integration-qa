import { createApp } from './app.js';
import { LoyaltyStore } from './domain/store.js';
import { InMemoryEventLog } from './events.js';
import { LoyaltyService } from './service.js';

const port = Number(process.env.PORT ?? 3001);
// Simulated write latency. It must be longer than the spread between parallel
// requests arriving (about 10-15 ms locally), or the concurrency tests can pass
// against a broken implementation just because the requests did not overlap.
const processingDelayMs = Number(process.env.LOYALTY_PROCESSING_DELAY_MS ?? 50);

const service = new LoyaltyService(new LoyaltyStore(), new InMemoryEventLog(), { processingDelayMs });
const server = createApp(service).listen(port, () => {
  console.log(`[loyalty-engine] listening on http://localhost:${port} (processing delay ${processingDelayMs} ms)`);
});

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => server.close(() => process.exit(0)));
}

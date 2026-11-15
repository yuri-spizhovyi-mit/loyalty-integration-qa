import { KAFKA_TOPICS, KafkaRelay, kafkaBrokers } from '../../shared/kafka.js';
import { createApp } from './app.js';
import { LoyaltyStore } from './domain/store.js';
import { BrokerRelay, type EventPublisher, FanOutPublisher, InMemoryEventLog, type LoyaltyEvent } from './events.js';
import { LoyaltyService } from './service.js';

const port = Number(process.env.PORT ?? 3001);
// Simulated write latency. It must be longer than the spread between parallel
// requests arriving (about 10-15 ms locally), or the concurrency tests can pass
// against a broken implementation just because the requests did not overlap.
const processingDelayMs = Number(process.env.LOYALTY_PROCESSING_DELAY_MS ?? 50);
// Events go to Kafka when KAFKA_BROKERS is set, else to the in-memory broker
// at BROKER_URL. Without either, the engine still records events in its
// outbox; the REST and contract suites do not need the rest of the stack.
const brokerUrl = process.env.BROKER_URL;
const kafka = kafkaBrokers();

const outbox = new InMemoryEventLog();
const relay: EventPublisher | null = kafka
  ? new KafkaRelay<LoyaltyEvent>('loyalty-engine', kafka, KAFKA_TOPICS.loyalty)
  : brokerUrl
    ? new BrokerRelay(brokerUrl)
    : null;
const publisher: EventPublisher = relay ? new FanOutPublisher(outbox, relay) : outbox;
const transport = kafka ? `Kafka ${kafka.join(',')}` : (brokerUrl ?? 'none');
const service = new LoyaltyService(new LoyaltyStore(), publisher, { processingDelayMs });
const server = createApp(service, outbox).listen(port, () => {
  console.log(
    `[loyalty-engine] listening on http://localhost:${port} (processing delay ${processingDelayMs} ms, ` +
      `events -> ${transport})`,
  );
});

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => server.close(() => process.exit(0)));
}

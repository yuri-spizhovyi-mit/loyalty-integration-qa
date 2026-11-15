import { ConsumerRunner, KAFKA_TOPICS, KafkaRelay, kafkaBrokers } from '../../shared/kafka.js';
import { createApp, eventHandler } from './app.js';
import { FaultInjector } from './faults.js';
import { ProfileStore } from './profiles.js';
import { BrokerRelay, NoopPublisher, type ProfileEventPublisher, type ProfileTierChanged } from './publisher.js';

const port = Number(process.env.PORT ?? 3002);
const brokerUrl = process.env.BROKER_URL;
const kafka = kafkaBrokers();

// Kafka when KAFKA_BROKERS is set; otherwise the in-memory broker pushes
// events to POST /events and profile events go back to it over HTTP.
const publisher: ProfileEventPublisher = kafka
  ? new KafkaRelay<ProfileTierChanged>('cdp', kafka, KAFKA_TOPICS.profile)
  : brokerUrl
    ? new BrokerRelay(brokerUrl)
    : new NoopPublisher();

const profiles = new ProfileStore();
const faults = new FaultInjector();
const consumer = kafka
  ? new ConsumerRunner({
      service: 'cdp',
      brokers: kafka,
      group: 'cdp.loyalty-events',
      topic: KAFKA_TOPICS.loyalty,
      handler: eventHandler(profiles, faults, publisher),
      maxAttempts: Number(process.env.BROKER_MAX_ATTEMPTS ?? 4),
      retryBaseMs: Number(process.env.BROKER_RETRY_BASE_MS ?? 100),
    })
  : undefined;

const server = createApp(profiles, faults, publisher, consumer).listen(port, () => {
  console.log(`[cdp] listening on http://localhost:${port}`);
  console.log(`[cdp]   transport: ${kafka ? `Kafka ${kafka.join(',')}` : `in-memory broker ${brokerUrl ?? '(none)'}`}`);
});
void consumer?.start();

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    void consumer?.close();
    server.close(() => process.exit(0));
  });
}

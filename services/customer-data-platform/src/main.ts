import { ConsumerRunner, KAFKA_TOPICS, retryModeFromEnv, KafkaRelay, kafkaBrokers } from '../../shared/kafka.js';
import { createApp, eventHandler } from './app.js';
import { FaultInjector } from './faults.js';
import { ProfileStore, type ProfileRepository } from './profiles.js';
import { BrokerRelay, NoopPublisher, type ProfileEventPublisher, type ProfileTierChanged } from './publisher.js';
import { OutboxKafkaRelay, SqliteProfileStore } from './sqlite-store.js';

const port = Number(process.env.PORT ?? 3002);
const brokerUrl = process.env.BROKER_URL;
const kafka = kafkaBrokers();

const dbPath = process.env.CDP_DB_PATH;

// Kafka when KAFKA_BROKERS is set; otherwise the in-memory broker pushes
// events to POST /events and profile events go back to it over HTTP.
// With CDP_DB_PATH (Kafka only), profiles and the outbox live in SQLite, so
// they survive a restart and can be shared by several instances.
let profiles: ProfileRepository;
let publisher: ProfileEventPublisher;
if (kafka && dbPath) {
  const store = new SqliteProfileStore(dbPath);
  profiles = store;
  publisher = new OutboxKafkaRelay(store, kafka, KAFKA_TOPICS.profile);
} else {
  profiles = new ProfileStore();
  publisher = kafka
    ? new KafkaRelay<ProfileTierChanged>('cdp', kafka, KAFKA_TOPICS.profile)
    : brokerUrl
      ? new BrokerRelay(brokerUrl)
      : new NoopPublisher();
}
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
      retryMode: retryModeFromEnv(),
      instanceId: process.env.CONSUMER_INSTANCE_ID,
    })
  : undefined;

const server = createApp(profiles, faults, publisher, consumer).listen(port, () => {
  console.log(`[cdp] listening on http://localhost:${port}`);
  console.log(`[cdp]   transport: ${kafka ? `Kafka ${kafka.join(',')}` : `in-memory broker ${brokerUrl ?? '(none)'}`}`);
  console.log(`[cdp]   profiles: ${kafka && dbPath ? `SQLite ${dbPath}` : 'in memory'}`);
});
void consumer?.start();

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    void consumer?.close();
    server.close(() => process.exit(0));
  });
}

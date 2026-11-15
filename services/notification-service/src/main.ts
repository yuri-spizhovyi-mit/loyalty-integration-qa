import { ConsumerRunner, KAFKA_TOPICS, kafkaBrokers } from '../../shared/kafka.js';
import { createApp, eventHandler } from './app.js';
import { FaultInjector } from './faults.js';
import { NotificationStore } from './notifications.js';

const port = Number(process.env.PORT ?? 3003);
const kafka = kafkaBrokers();

const store = new NotificationStore();
const faults = new FaultInjector();
// On Kafka the service reads profile.events itself; otherwise the in-memory
// broker pushes them to POST /events.
const consumer = kafka
  ? new ConsumerRunner({
      service: 'notifications',
      brokers: kafka,
      group: 'notifications.profile-events',
      topic: KAFKA_TOPICS.profile,
      handler: eventHandler(store, faults),
      maxAttempts: Number(process.env.BROKER_MAX_ATTEMPTS ?? 4),
      retryBaseMs: Number(process.env.BROKER_RETRY_BASE_MS ?? 100),
    })
  : undefined;

const server = createApp(store, faults, consumer).listen(port, () => {
  console.log(`[notifications] listening on http://localhost:${port}`);
  console.log(`[notifications]   transport: ${kafka ? `Kafka ${kafka.join(',')}` : 'in-memory broker (push)'}`);
});
void consumer?.start();

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    void consumer?.close();
    server.close(() => process.exit(0));
  });
}

import { KAFKA_TOPICS, kafkaBrokers } from '../../shared/kafka.js';
import { createApp } from './app.js';
import { Broker, type MessageBroker, type QueueBinding } from './broker.js';
import { KafkaBroker } from './kafka-broker.js';

const port = Number(process.env.PORT ?? 3004);
const cdpUrl = process.env.CDP_URL ?? 'http://localhost:3002';
const notificationUrl = process.env.NOTIFICATION_URL ?? 'http://localhost:3003';
const kafka = kafkaBrokers();

// Engine -> CDP, then CDP -> notification service. Each queue has its own
// retries and dead-letter queue (on Kafka: consumer group and DLT topic).
const queues = [
  { queue: 'cdp.loyalty-events', topic: KAFKA_TOPICS.loyalty, consumerUrl: cdpUrl },
  { queue: 'notifications.profile-events', topic: KAFKA_TOPICS.profile, consumerUrl: notificationUrl },
];

let broker: MessageBroker;
if (kafka) {
  const kafkaBroker = new KafkaBroker(queues, kafka);
  void kafkaBroker.start();
  broker = kafkaBroker;
} else {
  const bindings: QueueBinding[] = queues.map(({ queue, topic, consumerUrl }) => ({
    queue,
    topic,
    subscriberUrl: `${consumerUrl}/events`,
  }));
  broker = new Broker(bindings, {
    maxAttempts: Number(process.env.BROKER_MAX_ATTEMPTS ?? 4),
    retryBaseMs: Number(process.env.BROKER_RETRY_BASE_MS ?? 100),
    deliveryTimeoutMs: Number(process.env.BROKER_DELIVERY_TIMEOUT_MS ?? 2000),
  });
}

const server = createApp(broker).listen(port, () => {
  console.log(`[broker] listening on http://localhost:${port} (backend: ${broker.backend})`);
  for (const q of queues) console.log(`[broker]   ${q.topic} -> ${q.queue} -> ${q.consumerUrl}`);
});

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => server.close(() => process.exit(0)));
}

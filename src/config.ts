/** Where the services under test live. Defaults match `npm run start:*`. */
export const config = {
  loyaltyUrl: process.env.LOYALTY_URL ?? 'http://localhost:3001',
  cdpUrl: process.env.CDP_URL ?? 'http://localhost:3002',
  notificationUrl: process.env.NOTIFICATION_URL ?? 'http://localhost:3003',
  brokerUrl: process.env.BROKER_URL ?? 'http://localhost:3004',
  /**
   * Which message bus the stack runs on: 'memory' (default, the in-memory
   * broker) or 'kafka' (docker-compose.kafka.yml). The kafka test project
   * runs only on Kafka; everything else runs on both.
   */
  bus: process.env.BUS === 'kafka' ? ('kafka' as const) : ('memory' as const),
  /**
   * Chaos tests (tests/chaos): stop Kafka, crash and scale consumers through
   * Docker Compose. Only with CHAOS=true on Kafka, as in CI's Kafka legs.
   */
  chaos: process.env.CHAOS === 'true',
  /** The second CDP instance (compose profile "scale"), for rebalance tests. */
  cdp2Url: process.env.CDP2_URL ?? 'http://localhost:3012',
  /** On Kafka: how the consumers retry. Must match the services' KAFKA_RETRY_MODE. */
  kafkaRetryMode: process.env.KAFKA_RETRY_MODE === 'topics' ? ('topics' as const) : ('blocking' as const),
  /** Must match the broker's BROKER_MAX_ATTEMPTS. */
  brokerMaxAttempts: Number(process.env.BROKER_MAX_ATTEMPTS ?? 4),
} as const;

/** Broker queue that feeds the customer data platform. */
export const CDP_QUEUE = 'cdp.loyalty-events';
export const LOYALTY_TOPIC = 'loyalty.events';

/** Broker queue that feeds the notification service from the customer data platform. */
export const NOTIFICATION_QUEUE = 'notifications.profile-events';
export const PROFILE_TOPIC = 'profile.events';

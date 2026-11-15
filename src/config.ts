/** Where the services under test live. Defaults match `npm run start:*`. */
export const config = {
  loyaltyUrl: process.env.LOYALTY_URL ?? 'http://localhost:3001',
  cdpUrl: process.env.CDP_URL ?? 'http://localhost:3002',
  notificationUrl: process.env.NOTIFICATION_URL ?? 'http://localhost:3003',
  brokerUrl: process.env.BROKER_URL ?? 'http://localhost:3004',
  /** Must match the broker's BROKER_MAX_ATTEMPTS. */
  brokerMaxAttempts: Number(process.env.BROKER_MAX_ATTEMPTS ?? 4),
} as const;

/** Broker queue that feeds the customer data platform. */
export const CDP_QUEUE = 'cdp.loyalty-events';
export const LOYALTY_TOPIC = 'loyalty.events';

/** Broker queue that feeds the notification service from the customer data platform. */
export const NOTIFICATION_QUEUE = 'notifications.profile-events';
export const PROFILE_TOPIC = 'profile.events';

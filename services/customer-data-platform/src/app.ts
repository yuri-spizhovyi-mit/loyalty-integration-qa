import express, { type NextFunction, type Request, type Response } from 'express';
import { InvalidEventError, parseEvent } from './events.js';
import type { FaultInjector, FaultMode } from './faults.js';
import type { ProfileRepository } from './profiles.js';
import { type ProfileEventPublisher, profileTierChanged } from './publisher.js';

const PROFILE_ID = /^[A-Za-z0-9_-]{3,64}$/;
const FAULT_MODES: readonly FaultMode[] = ['fail-before-apply', 'fail-after-apply', 'crash-after-apply'];
const MAX_FAULT_TIMES = 1000;

interface ErrorDetails {
  code: string;
  message: string;
  details?: string[];
}

function sendError(res: Response, status: number, error: ErrorDetails) {
  res.status(status).json({ error, correlationId: (res.locals.correlationId as string | undefined) ?? null });
}

export interface EventResult {
  status: number;
  body: unknown;
}

/** Delivery records of the Kafka consumer, when the service runs on Kafka. */
export interface DeliveryLookup {
  delivery(messageId: string): unknown;
  lag(): Promise<number[]>;
  committedOffsets(): Record<string, string>;
  readonly retryMode: string;
  readonly assignedPartitions: number[];
  readonly streamRestarts: number;
  failStream(): boolean;
  readonly ready: boolean;
}

/**
 * Handles one incoming event, however it arrived: pushed over HTTP by the
 * in-memory broker, or read from Kafka. The answer uses HTTP semantics in
 * both cases: 2xx = ack, 400 = never retry, 503 = retry.
 */
export function eventHandler(profiles: ProfileRepository, faults: FaultInjector, publisher: ProfileEventPublisher) {
  return (raw: string, meta: { messageId: string | null; correlationId: string | null }): EventResult => {
    const error = (status: number, details: ErrorDetails): EventResult => ({
      status,
      body: { error: details, correlationId: meta.correlationId },
    });
    let event;
    try {
      event = parseEvent(raw);
    } catch (err) {
      if (!(err instanceof InvalidEventError)) throw err;
      console.warn(`[cdp] rejected message ${meta.messageId ?? '-'}: ${err.message}`);
      return error(400, { code: err.code, message: 'Event rejected', details: err.problems });
    }

    const fault = faults.take(event.customerId);
    if (fault === 'fail-before-apply') {
      return error(503, { code: 'INJECTED_FAULT', message: 'Simulated failure before apply' });
    }
    // One transaction: the profile write and the outgoing event (outbox) happen
    // together or not at all. A lost ack or a crash below cannot lose the event.
    const { result } = profiles.transaction(() => {
      const outcome = profiles.apply(event);
      if (outcome.tierChange && event.type === 'tier.changed') {
        publisher.publish(profileTierChanged(event, outcome.tierChange.marketingConsent));
      }
      return outcome;
    });
    if (fault === 'crash-after-apply') {
      console.warn(`[cdp] injected crash after applying ${event.eventId}`);
      process.exit(1);
    }
    if (fault === 'fail-after-apply') {
      return error(503, { code: 'INJECTED_FAULT', message: 'Simulated failure after apply (ack lost)' });
    }
    return { status: 200, body: { eventId: event.eventId, result } };
  };
}

export function createApp(
  profiles: ProfileRepository,
  faults: FaultInjector,
  publisher: ProfileEventPublisher,
  consumer?: DeliveryLookup,
) {
  const handle = eventHandler(profiles, faults, publisher);
  const app = express();
  app.disable('x-powered-by');

  app.use((req, res, next) => {
    const correlationId = req.header('X-Correlation-ID');
    if (correlationId) {
      res.locals.correlationId = correlationId;
      res.setHeader('X-Correlation-ID', correlationId);
    }
    next();
  });

  app.get('/health', (_req, res) => {
    // On Kafka, not ready until the consumer group is joined.
    if (consumer && !consumer.ready) return sendError(res, 503, { code: 'NOT_READY', message: 'Consumer not ready' });
    res.json({ status: 'ok', service: 'customer-data-platform' });
  });

  // Push endpoint for the in-memory broker, and for tests that bypass any broker.
  app.post('/events', express.text({ type: '*/*', limit: '64kb' }), (req, res) => {
    const raw = typeof req.body === 'string' ? req.body : '';
    const { status, body } = handle(raw, {
      messageId: req.header('X-Message-Id') ?? null,
      correlationId: (res.locals.correlationId as string | undefined) ?? null,
    });
    res.status(status).json(body);
  });

  // Kafka only: what the consumer did with each record, for the broker's inspection API.
  app.get('/internal/deliveries/:messageId', (req, res) => {
    const delivery = consumer?.delivery(req.params.messageId as string);
    if (!delivery) return sendError(res, 404, { code: 'NOT_FOUND', message: 'No delivery recorded' });
    res.json(delivery);
  });

  // Kafka only, test only: break the consumer's stream; the supervisor must restart it.
  app.post('/internal/consumer/stream-failure', (_req, res) => {
    if (!consumer?.failStream()) return sendError(res, 409, { code: 'NOT_FOUND', message: 'No running consumer stream' });
    res.status(202).json({ injected: true });
  });

  app.get('/internal/consumer', async (_req, res) => {
    if (!consumer) return sendError(res, 404, { code: 'NOT_FOUND', message: 'Not running on Kafka' });
    res.json({
      ready: consumer.ready,
      retryMode: consumer.retryMode,
      assignedPartitions: consumer.assignedPartitions,
      streamRestarts: consumer.streamRestarts,
      lag: await consumer.lag(),
      committedOffsets: consumer.committedOffsets(),
    });
  });

  app.get('/profiles/:profileId', (req, res) => {
    const profile = profiles.get(req.params.profileId as string);
    if (!profile) {
      return sendError(res, 404, { code: 'PROFILE_NOT_FOUND', message: `Profile ${req.params.profileId} not found` });
    }
    res.json(profile);
  });

  app.put('/internal/faults/:customerId', express.json(), (req, res) => {
    const customerId = req.params.customerId as string;
    const body = (req.body ?? {}) as { mode?: unknown; times?: unknown };
    const problems: string[] = [];
    if (!PROFILE_ID.test(customerId)) problems.push('customerId is invalid');
    if (!FAULT_MODES.includes(body.mode as FaultMode)) problems.push(`mode must be one of ${FAULT_MODES.join(', ')}`);
    if (!Number.isInteger(body.times) || (body.times as number) < 1 || (body.times as number) > MAX_FAULT_TIMES) {
      problems.push(`times must be an integer from 1 to ${MAX_FAULT_TIMES}`);
    }
    if (problems.length > 0) {
      return sendError(res, 400, { code: 'VALIDATION_ERROR', message: 'Fault is invalid', details: problems });
    }
    faults.set(customerId, { mode: body.mode as FaultMode, remaining: body.times as number });
    res.status(204).end();
  });

  app.delete('/internal/faults/:customerId', (req, res) => {
    faults.clear(req.params.customerId as string);
    res.status(204).end();
  });

  app.use((req, res) => sendError(res, 404, { code: 'NOT_FOUND', message: `No route for ${req.method} ${req.path}` }));

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (typeof err === 'object' && err !== null && (err as { type?: string }).type === 'entity.parse.failed') {
      return sendError(res, 400, { code: 'MALFORMED_JSON', message: 'Request body is not valid JSON' });
    }
    console.error('[cdp] unhandled error', err);
    sendError(res, 500, { code: 'INTERNAL_ERROR', message: 'Unexpected server error' });
  });

  return app;
}

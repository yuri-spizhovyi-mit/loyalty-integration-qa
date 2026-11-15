import express, { type NextFunction, type Request, type Response } from 'express';
import { InvalidEventError, parseEvent } from './events.js';
import type { FaultInjector, FaultMode } from './faults.js';
import type { ProfileStore } from './profiles.js';

const PROFILE_ID = /^[A-Za-z0-9_-]{3,64}$/;
const FAULT_MODES: readonly FaultMode[] = ['fail-before-apply', 'fail-after-apply'];
const MAX_FAULT_TIMES = 1000;

interface ErrorDetails {
  code: string;
  message: string;
  details?: string[];
}

function sendError(res: Response, status: number, error: ErrorDetails) {
  res.status(status).json({ error, correlationId: (res.locals.correlationId as string | undefined) ?? null });
}

export function createApp(profiles: ProfileStore, faults: FaultInjector) {
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
    res.json({ status: 'ok', service: 'customer-data-platform' });
  });

  // Push endpoint for the broker. 2xx = ack, 400 = never retry, 503 = retry.
  app.post('/events', express.text({ type: '*/*', limit: '64kb' }), (req, res) => {
    const raw = typeof req.body === 'string' ? req.body : '';
    let event;
    try {
      event = parseEvent(raw);
    } catch (err) {
      if (!(err instanceof InvalidEventError)) throw err;
      console.warn(`[cdp] rejected message ${req.header('X-Message-Id') ?? '-'}: ${err.message}`);
      return sendError(res, 400, { code: err.code, message: 'Event rejected', details: err.problems });
    }

    const fault = faults.take(event.customerId);
    if (fault === 'fail-before-apply') {
      return sendError(res, 503, { code: 'INJECTED_FAULT', message: 'Simulated failure before apply' });
    }
    const result = profiles.apply(event);
    if (fault === 'fail-after-apply') {
      return sendError(res, 503, { code: 'INJECTED_FAULT', message: 'Simulated failure after apply (ack lost)' });
    }
    res.json({ eventId: event.eventId, result });
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

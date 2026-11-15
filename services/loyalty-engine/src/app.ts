import { randomUUID } from 'node:crypto';
import express, { type NextFunction, type Request, type Response } from 'express';
import { canonicalJson, IdempotencyStore, type StoredResponse } from './concurrency.js';
import { ApiError, errorBody } from './errors.js';
import type { InMemoryEventLog } from './events.js';
import type { LoyaltyService } from './service.js';
import {
  CUSTOMER_ID_PATTERN,
  parseCreateCustomer,
  parseIdempotencyKey,
  parsePointsRequest,
  parseRefundRequest,
} from './validation.js';

const MAX_CORRELATION_ID_LENGTH = 128;

function correlationIdOf(res: Response): string {
  return res.locals.correlationId as string;
}

export function createApp(service: LoyaltyService, outbox: InMemoryEventLog) {
  const app = express();
  const idempotency = new IdempotencyStore();

  app.disable('x-powered-by');
  app.use(express.json({ limit: '16kb' }));

  // Every response carries X-Correlation-ID: the caller's, or a new one.
  app.use((req, res, next) => {
    const incoming = req.header('X-Correlation-ID');
    const correlationId =
      incoming && incoming.length <= MAX_CORRELATION_ID_LENGTH ? incoming : `corr_${randomUUID()}`;
    res.locals.correlationId = correlationId;
    res.setHeader('X-Correlation-ID', correlationId);
    next();
  });

  /**
   * Wraps a ledger write with Idempotency-Key handling. The fingerprint is
   * method + path + canonical body, so the same key on a different request
   * is detected as reuse.
   */
  async function writeWithIdempotency(req: Request, res: Response, execute: () => Promise<StoredResponse>) {
    const key = parseIdempotencyKey(req.header('Idempotency-Key'));
    if (!key) {
      const result = await execute();
      res.status(result.status).json(result.body);
      return;
    }
    const fingerprint = `${req.method} ${req.path} ${canonicalJson(req.body)}`;
    const result = await idempotency.run(key, fingerprint, execute);
    res.setHeader('Idempotent-Replayed', String(result.replayed));
    res.status(result.status).json(result.body);
  }

  app.get('/health', (_req, res) => {
    res.json({ status: 'ok', service: 'loyalty-engine' });
  });

  app.post('/customers', (req, res) => {
    res.status(201).json(service.createCustomer(parseCreateCustomer(req.body), correlationIdOf(res)));
  });

  app.get('/customers/:customerId', (req, res) => {
    res.json(service.customerSummary(req.params.customerId));
  });

  app.get('/customers/:customerId/tier', (req, res) => {
    res.json(service.tier(req.params.customerId));
  });

  app.get('/customers/:customerId/ledger', (req, res) => {
    res.json(service.ledger(req.params.customerId));
  });

  app.post('/customers/:customerId/earn', async (req, res) => {
    const request = parsePointsRequest(req.body);
    await writeWithIdempotency(req, res, async () => ({
      status: 201,
      body: await service.earn(req.params.customerId as string, request, correlationIdOf(res)),
    }));
  });

  app.post('/customers/:customerId/redeem', async (req, res) => {
    const request = parsePointsRequest(req.body);
    await writeWithIdempotency(req, res, async () => ({
      status: 201,
      body: await service.redeem(req.params.customerId as string, request, correlationIdOf(res)),
    }));
  });

  app.post('/customers/:customerId/refund', async (req, res) => {
    const request = parseRefundRequest(req.body);
    await writeWithIdempotency(req, res, async () => ({
      status: 201,
      body: await service.refund(req.params.customerId as string, request, correlationIdOf(res)),
    }));
  });

  // Test inspection: the events published for one customer, in publish order.
  app.get('/internal/outbox', (req, res) => {
    const customerId = req.query.customerId;
    if (typeof customerId !== 'string' || !CUSTOMER_ID_PATTERN.test(customerId)) {
      throw new ApiError('VALIDATION_ERROR', 'customerId query parameter is required', [
        { field: 'customerId', message: 'must be a valid customer ID' },
      ]);
    }
    res.json({ customerId, events: outbox.forCustomer(customerId) });
  });

  app.use((req, _res, next) => {
    next(new ApiError('NOT_FOUND', `No route for ${req.method} ${req.path}`));
  });

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    const correlationId = correlationIdOf(res) ?? 'unknown';
    if (err instanceof ApiError) {
      res.status(err.status).json(errorBody(err, correlationId));
      return;
    }
    if (isBodyParseError(err)) {
      res.status(400).json(errorBody(new ApiError('MALFORMED_JSON', 'Request body is not valid JSON'), correlationId));
      return;
    }
    console.error(`[loyalty-engine] unhandled error correlationId=${correlationId}`, err);
    res.status(500).json(errorBody(new ApiError('INTERNAL_ERROR', 'Unexpected server error'), correlationId));
  });

  return app;
}

function isBodyParseError(err: unknown): boolean {
  return typeof err === 'object' && err !== null && (err as { type?: string }).type === 'entity.parse.failed';
}

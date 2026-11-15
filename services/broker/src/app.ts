import express, { type NextFunction, type Request, type Response } from 'express';
import type { Broker } from './broker.js';

function error(res: Response, status: number, code: string, message: string) {
  res.status(status).json({ error: { code, message } });
}

export function createApp(broker: Broker) {
  const app = express();
  app.disable('x-powered-by');
  // Payloads are stored as raw text so tests can publish malformed JSON.
  app.use(express.text({ type: '*/*', limit: '64kb' }));

  app.get('/health', (_req, res) => {
    res.json({ status: 'ok', service: 'broker' });
  });

  app.post('/topics/:topic/messages', (req, res) => {
    const topic = req.params.topic as string;
    if (!broker.hasTopic(topic)) return error(res, 404, 'TOPIC_NOT_FOUND', `No queue is bound to topic ${topic}`);
    const payload = typeof req.body === 'string' ? req.body : '';
    if (payload.length === 0) return error(res, 400, 'EMPTY_MESSAGE', 'Message body is empty');
    const message = broker.publish(topic, payload, req.header('X-Correlation-ID') ?? null);
    res.status(202).json({ messageId: message.messageId, queues: message.deliveries.map((d) => d.queue) });
  });

  app.get('/topics/:topic/messages', (req, res) => {
    const topic = req.params.topic as string;
    if (!broker.hasTopic(topic)) return error(res, 404, 'TOPIC_NOT_FOUND', `No queue is bound to topic ${topic}`);
    const customerId = typeof req.query.customerId === 'string' ? req.query.customerId : undefined;
    res.json({ topic, messages: broker.messagesOn(topic, customerId) });
  });

  app.get('/messages/:messageId', (req, res) => {
    const message = broker.getMessage(req.params.messageId as string);
    if (!message) return error(res, 404, 'MESSAGE_NOT_FOUND', `Message ${req.params.messageId} not found`);
    res.json(message);
  });

  app.get('/queues', (_req, res) => {
    res.json({ queues: broker.stats() });
  });

  app.get('/queues/:queue/dead-letters', (req, res) => {
    const queue = req.params.queue as string;
    if (!broker.hasQueue(queue)) return error(res, 404, 'QUEUE_NOT_FOUND', `Queue ${queue} not found`);
    const customerId = typeof req.query.customerId === 'string' ? req.query.customerId : undefined;
    const messageId = typeof req.query.messageId === 'string' ? req.query.messageId : undefined;
    res.json({ queue, messages: broker.deadLetters(queue, { customerId, messageId }) });
  });

  app.post('/queues/:queue/dead-letters/:messageId/redrive', (req, res) => {
    const queue = req.params.queue as string;
    const message = broker.redrive(queue, req.params.messageId as string);
    if (!message) {
      return error(res, 404, 'DEAD_LETTER_NOT_FOUND', `No dead-lettered message ${req.params.messageId} on ${queue}`);
    }
    res.status(202).json({ messageId: message.messageId, queue });
  });

  app.use((req, res) => error(res, 404, 'NOT_FOUND', `No route for ${req.method} ${req.path}`));

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    console.error('[broker] unhandled error', err);
    error(res, 500, 'INTERNAL_ERROR', 'Unexpected server error');
  });

  return app;
}

import { createApp } from './app.js';
import { FaultInjector } from './faults.js';
import { NotificationStore } from './notifications.js';

const port = Number(process.env.PORT ?? 3003);

const server = createApp(new NotificationStore(), new FaultInjector()).listen(port, () => {
  console.log(`[notifications] listening on http://localhost:${port}`);
});

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => server.close(() => process.exit(0)));
}

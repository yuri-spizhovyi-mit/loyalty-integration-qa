import { createApp } from './app.js';
import { FaultInjector } from './faults.js';
import { ProfileStore } from './profiles.js';

const port = Number(process.env.PORT ?? 3002);

const server = createApp(new ProfileStore(), new FaultInjector()).listen(port, () => {
  console.log(`[cdp] listening on http://localhost:${port}`);
});

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => server.close(() => process.exit(0)));
}

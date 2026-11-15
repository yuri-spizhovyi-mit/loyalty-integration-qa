import { createApp } from './app.js';
import { FaultInjector } from './faults.js';
import { ProfileStore } from './profiles.js';
import { BrokerRelay, NoopPublisher } from './publisher.js';

const port = Number(process.env.PORT ?? 3002);
const brokerUrl = process.env.BROKER_URL;
const publisher = brokerUrl ? new BrokerRelay(brokerUrl) : new NoopPublisher();

const server = createApp(new ProfileStore(), new FaultInjector(), publisher).listen(port, () => {
  console.log(`[cdp] listening on http://localhost:${port}`);
  console.log(`[cdp]   profile events -> ${brokerUrl ?? '(no broker)'}`);
});

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => server.close(() => process.exit(0)));
}

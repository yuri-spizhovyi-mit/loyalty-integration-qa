import { hostname } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { setTimeout as sleep } from 'node:timers/promises';
import { ProduceAcks, Producer, stringSerializers } from '@platformatic/kafka';
import { newMessageId } from '../../shared/kafka.js';
import type { LoyaltyEvent } from './events.js';
import {
  type ApplyOutcome,
  applyNewEvent,
  newProfileState,
  type Profile,
  type ProfileRepository,
  type ProfileState,
  recordDuplicate,
} from './profiles.js';
import type { ProfileEventPublisher, ProfileTierChanged } from './publisher.js';

/**
 * Profiles in SQLite, used when the platform runs on Kafka (CDP_DB_PATH).
 * The state survives a restart, and several instances can share the file
 * (on one host, through a Docker volume), so a consumer group can rebalance
 * partitions between them without losing anything.
 *
 * One transaction covers everything an event changes: the seen-event row
 * (deduplication), the profile, and the outgoing profile event (outbox).
 * After a crash, either all of it happened or none of it did. A redelivered
 * event then finds its seen-event row and is a duplicate.
 *
 * node:sqlite is still marked experimental in Node 24; it is enough for a
 * mock, and keeps the stack free of a database server.
 */
export class SqliteProfileStore implements ProfileRepository {
  private readonly db: DatabaseSync;
  private depth = 0;

  constructor(path: string) {
    this.db = new DatabaseSync(path);
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA busy_timeout = 5000;
      CREATE TABLE IF NOT EXISTS profiles (profile_id TEXT PRIMARY KEY, state TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS seen_events (event_id TEXT PRIMARY KEY, profile_id TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT NOT NULL,
        payload TEXT NOT NULL,
        claimed_by TEXT,
        claimed_until INTEGER
      );
    `);
  }

  get(profileId: string): Profile | undefined {
    return this.load(profileId)?.profile;
  }

  apply(event: LoyaltyEvent): ApplyOutcome {
    return this.transaction(() => {
      const state = this.load(event.customerId) ?? newProfileState(event.customerId);
      const seen = this.db.prepare('SELECT 1 FROM seen_events WHERE event_id = ?').get(event.eventId);
      const outcome = seen ? recordDuplicate(state) : applyNewEvent(state, event);
      if (!seen) this.db.prepare('INSERT INTO seen_events (event_id, profile_id) VALUES (?, ?)').run(event.eventId, event.customerId);
      this.save(state);
      return outcome;
    });
  }

  /** BEGIN IMMEDIATE takes the write lock at once, so two instances cannot interleave. Nested calls join the outer one. */
  transaction<T>(work: () => T): T {
    if (this.depth > 0) return work();
    this.depth += 1;
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const result = work();
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    } finally {
      this.depth -= 1;
    }
  }

  // ---------------------------------------------------------------- outbox

  enqueue(payload: string): void {
    this.db.prepare('INSERT INTO outbox (message_id, payload) VALUES (?, ?)').run(newMessageId(), payload);
  }

  /** Claims the oldest unclaimed rows (or rows whose claim expired) for this instance. */
  claim(owner: string, limit: number, leaseMs: number): { id: number; messageId: string; payload: string }[] {
    return this.transaction(() => {
      const now = Date.now();
      const rows = this.db
        .prepare(
          'SELECT id, message_id, payload FROM outbox WHERE claimed_by IS NULL OR claimed_until < ? OR claimed_by = ? ORDER BY id LIMIT ?',
        )
        .all(now, owner, limit) as { id: number; message_id: string; payload: string }[];
      const update = this.db.prepare('UPDATE outbox SET claimed_by = ?, claimed_until = ? WHERE id = ?');
      for (const row of rows) update.run(owner, now + leaseMs, row.id);
      return rows.map((row) => ({ id: row.id, messageId: row.message_id, payload: row.payload }));
    });
  }

  remove(id: number): void {
    this.db.prepare('DELETE FROM outbox WHERE id = ?').run(id);
  }

  // ---------------------------------------------------------------- state

  private load(profileId: string): ProfileState | undefined {
    const row = this.db.prepare('SELECT state FROM profiles WHERE profile_id = ?').get(profileId) as { state: string } | undefined;
    if (!row) return undefined;
    const stored = JSON.parse(row.state) as Omit<ProfileState, 'sequences'> & { sequences: number[] };
    return { ...stored, sequences: new Set(stored.sequences) };
  }

  private save(state: ProfileState): void {
    const json = JSON.stringify({ ...state, sequences: [...state.sequences] });
    this.db
      .prepare('INSERT INTO profiles (profile_id, state) VALUES (?, ?) ON CONFLICT(profile_id) DO UPDATE SET state = excluded.state')
      .run(state.profile.profileId, json);
  }
}

/**
 * Transactional outbox relay: `publish` writes the profile event into the
 * outbox table inside the caller's transaction; a loop sends rows to Kafka
 * in order and deletes each one after Kafka accepts it. Rows written before
 * a crash are sent after the restart. With several instances, each row is
 * claimed by one of them for a short lease.
 */
export class OutboxKafkaRelay implements ProfileEventPublisher {
  private readonly producer: Producer<string, string, string, string>;
  private readonly owner = `${hostname()}-${process.pid}`;
  private draining = false;

  constructor(
    private readonly store: SqliteProfileStore,
    brokers: string[],
    private readonly topic: string,
  ) {
    this.producer = new Producer({
      clientId: 'cdp-outbox',
      bootstrapBrokers: brokers,
      serializers: stringSerializers,
      idempotent: true,
      acks: ProduceAcks.ALL,
      autocreateTopics: false,
    });
    // Leftovers from before a restart, and rows whose claim expired on another instance.
    setInterval(() => void this.drain(), 1000).unref();
  }

  publish(event: ProfileTierChanged): void {
    this.store.enqueue(JSON.stringify(event));
    // After the surrounding transaction has committed.
    setImmediate(() => void this.drain());
  }

  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    try {
      for (;;) {
        const rows = this.store.claim(this.owner, 50, 10_000);
        if (rows.length === 0) return;
        for (const row of rows) {
          const event = JSON.parse(row.payload) as ProfileTierChanged;
          for (;;) {
            try {
              await this.producer.send({
                messages: [
                  {
                    topic: this.topic,
                    key: event.customerId,
                    value: row.payload,
                    headers: { 'message-id': row.messageId, 'correlation-id': event.correlationId },
                  },
                ],
              });
              break;
            } catch (error) {
              console.error(`[cdp] outbox produce failed, retrying: ${String(error)}`);
              await sleep(200);
            }
          }
          this.store.remove(row.id);
        }
      }
    } finally {
      this.draining = false;
    }
  }
}

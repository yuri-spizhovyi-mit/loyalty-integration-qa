# Test strategy

This document covers how the loyalty integration is tested: what is in scope, which risks the tests target, when a build may be tested and released, the acceptance criteria per feature, and the trade-offs behind the design.

The system under test is described in the [README](../README.md). It has four mock services:

- the **loyalty engine** (REST, ledger, tiers);
- a **message bus** with retries and dead-letter queues: either an in-memory broker or **Kafka** (§7.5);
- the **customer data platform** (CDP), which builds customer profiles from events;
- the **notification service**, which records tier-change messages.

The specs in [`specs/`](../specs) describe each service's API or events and are the single source of truth for both the tests and the services.

## 1. Scope

### In scope

| Area | What is tested |
|---|---|
| Loyalty engine REST API | Customers, earn, redeem, refund, tier lookup, ledger: status codes, error bodies, validation and boundaries |
| Contracts | Every response against OpenAPI and every event against AsyncAPI. The specs are checked against each other, and against the services' own validation |
| Idempotency and concurrency | `Idempotency-Key` replays, key reuse, parallel requests with one key, parallel writes for one customer |
| Event flow | Engine → broker → CDP → broker → notification service, end to end, within a time limit |
| Event reliability | Duplicates, out-of-order delivery, lost acknowledgements, retries with backoff, dead-letter queues and redrive |
| Data integrity | Ledger reconciliation (sum of entries = balance), engine ↔ outbox ↔ profile reconciliation, field mapping per row |
| Notifications | Exactly one per tier change, none without consent, none for failed transactions |
| Kafka | Partitioning by key, order within a partition, two retry modes (blocking in place, and retry topics), offsets committed only after handling, dead-letter topic records, and recovery: a consumer crashing mid-record, Kafka going away, the consumer group growing and shrinking |
| Delivery pipeline | The same suite runs in GitHub Actions against Docker Compose in three configurations (in-memory broker, Kafka with blocking retries, Kafka with retry topics): on every push to `main`, every pull request and nightly |

### Out of scope

- **Real message delivery.** The notification service records messages; it sends no email or push. Template content and rendering are not tested.
- **Authentication and authorisation.** The mocks have none. Only one rule is tested: a customer cannot refund another customer's transaction.
- **Performance and load.** The parallel tests check correctness under contention, not throughput or latency percentiles. The timing limits used (see §7.1) are sanity checks on a single machine, not service-level objectives.
- **Persistence beyond the CDP.** On Kafka the CDP keeps its profiles and outbox in SQLite, so its restarts are tested (§7.12). The engine and the notification service keep their state in memory, so their restarts, data migration and backup are not covered.
- **Real infrastructure beyond one Kafka broker.** Kafka runs as a single node, so replication, broker failover and multi-broker rebalancing are not covered. There are no real vendor APIs, and no network faults beyond what the bus and fault injection simulate.
- **UI.** There is none; the tests use Playwright's `APIRequestContext` only.

## 2. Quality risks

The tests are prioritised by risk: what goes wrong in loyalty integrations, and what it costs when it does.

| # | Risk | Impact | Where it is tested |
|---|---|---|---|
| R1 | A balance is wrong: points added twice, lost or overdrawn | Direct financial loss, customer complaints | `tests/api/*`, `tests/integrity/ledger-reconciliation.spec.ts` |
| R2 | A retried request is applied twice (client timeout, double click) | Duplicate points or redemptions | `tests/api/idempotency.spec.ts` |
| R3 | Concurrent writes race and overdraw a balance | Financial loss | `tests/api/redeem.spec.ts`, `ledger-reconciliation.spec.ts` (parallel tests) |
| R4 | Wrong tier at a threshold (off-by-one, refunds not counted) | Wrong benefits, wrong messages | `tests/api/tiers.spec.ts`, `tests/api/refund.spec.ts` |
| R5 | Systems disagree on the shape of a payload | Integration breaks silently or loudly after a release | `tests/contract/*` |
| R6 | An event is lost between systems | Profile out of date, message never sent | `tests/events/profile-sync.spec.ts`, lost-ack and redrive tests |
| R7 | An event is processed twice (at-least-once delivery) | Double counting, duplicate messages | `tests/events/duplicates.spec.ts`, `tests/notifications/notification-reliability.spec.ts` |
| R8 | Events arrive out of order and an old value overwrites a new one | Profile shows a stale tier or balance | `tests/events/ordering.spec.ts`, stale-upgrade notification test |
| R9 | A malformed ("poison") message blocks the queue or is retried forever | All later events stall | `tests/events/dead-letter.spec.ts`, notification DLQ tests |
| R10 | A field is mapped wrongly between systems (case, type, wrong source field) | Bad segmentation, wrong personalisation | `tests/integrity/field-mapping.spec.ts` |
| R11 | A message goes to a customer without marketing consent | Regulatory exposure (consent law), complaints | `tests/notifications/*` |
| R12 | A customer gets no message, or two, for one tier change | Poor experience, support load | `tests/notifications/*` |
| R13 | A failed transaction still triggers downstream effects | Profile and messages disagree with the ledger | "failed transactions publish nothing" and "failed transactions send nothing" |
| R14 | Kafka specifics break an assumption: a customer's events land on different partitions and lose their order, an offset is committed before the record is handled (lost on a crash), or a dead-letter record cannot be traced back | Out-of-order profiles, silently lost events, dead letters nobody can redrive | `tests/kafka/kafka-semantics.spec.ts` |
| R15 | Recovery goes wrong: a consumer crashes after writing but before committing and applies the event twice, an outgoing event is lost in the crash, a Kafka outage loses events, a consumer silently stops reading after an outage, or a rebalance loses or repeats records | Lost or doubled points and messages, a profile that stops updating while everything reports healthy | `tests/chaos/kafka-failures.spec.ts` |

## 3. Test approach

### Layers

The suite has seven Playwright projects. `contract` runs first, and the others depend on it (§7.2). The `kafka` project exists only when the stack runs on Kafka, and `chaos` only with `CHAOS=true` on Kafka (as in CI). Both run last, one test at a time (§7.11).

| Project | Folder | Tests | Purpose |
|---|---:|---:|---|
| contract | `tests/contract` | 161 | Specs are valid and consistent; every documented response and event is produced and matches its schema; services accept exactly what the specs allow |
| api | `tests/api` | 76 | Loyalty engine REST behaviour: functional, negative, boundary, idempotency, concurrency |
| events | `tests/events` | 29 | Profile sync end to end; duplicates; ordering; retries and dead-letter queue |
| integrity | `tests/integrity` | 35 | Ledger reconciliation, cross-system reconciliation, field mapping per row, self-test of the checker |
| notifications | `tests/notifications` | 20 | Notification triggers end to end, and exactly-once behaviour under faults |
| kafka | `tests/kafka` | 5 + 3 | Kafka only. 5 in both retry modes: the running mode, keys and partitions, dead-letter records. 3 for the running mode: head-of-line blocking, commit timing and lag (blocking), or no blocking, parking and reordering (retry topics) |
| chaos | `tests/chaos` | 3 | Kafka with `CHAOS=true` only: the CDP crashing after applying but before committing, Kafka stopped and restarted, a second CDP instance joining and leaving the group |
| **Total** | | **321 + 8 + 3** | 321 on the in-memory broker; 329 on Kafka in either retry mode; 332 with the chaos tests. The first 321 run unchanged everywhere |

### Test design techniques

- **Boundary values.** Tier thresholds at ±1 point (999/1000, 4999/5000), points 1 and 1,000,000 and one past each, and length limits: reference 128/129, idempotency key 255/256, customer ID 3–64.
- **Equivalence classes and negative tests.** For each kind of bad input (wrong type, missing, unknown field, malformed JSON), one test checks that it is rejected and that nothing is written.
- **State-based tests.** Sequences like earn → redeem → refund, where the outcome depends on earlier steps: a refund after a redeem, a partial refund followed by a full one.
- **Contract parity.** The services validate input with their own hand-written code. The same bodies are sent to the service and to the spec validator, and both must agree (`request-parity`, and "spec and consumer both reject" for events).
- **Fault injection.** Test-only endpoints make the CDP or the notification service fail for one customer, either before processing (an outage) or after it (a lost acknowledgement). This drives the retry, duplicate and DLQ paths on purpose instead of waiting for them to happen.
- **Seeded random runs.** Ledger reconciliation runs random mixes of earns, redeems, refunds and retries with fixed seeds, so a failure can be replayed exactly. The seed is in the test title, and the operation log is attached to the report.
- **Independent oracles.** Expected tiers and ledger rules are written in the test code from the business rules (`src/helpers/reconciliation.ts`), never imported from the services.
- **Mutation testing (manual).** Bugs were injected into the services one at a time to show the tests catch them. Examples: no deduplication, last-arrival-wins, consent ignored, unknown consent treated as consent, publishing after the acknowledgement, a wrong `balanceAfter`, tier not lower-cased. Four more were Kafka-specific: committing before handling, losing per-partition order, a random key instead of the customer ID, and a dead-letter record without its source placement. Three more were for retry topics: retrying in place anyway, no delay on the retry topics, and a dead-letter record that points at the retry topic instead of the original. Three more were for recovery: deduplication not persisted, the outbox kept in memory, and no restart of a failed consumer stream. All 24 were caught. One planned recovery mutant, "the engine's relay drops events while Kafka is down", could not be reached: the Kafka client waits through an outage instead of failing the send, so the relay's error path never runs (§7.12). This is not automated yet (§9).

### Framework structure

- **Service objects** (`src/clients/`) wrap each API. With a contract validator attached, every response is checked against its OpenAPI schema before the test sees it. A wrong field fails the test even when the test only looks at a different field.
- **Builders** (`src/builders/`) create valid default data. Tests change only the field they care about.
- **Helpers** (`src/helpers/`) cover polling, the schema validator, the queue inspector, delivery waits, reconciliation and the seeded random generator.
- **Custom matchers**: `toHaveStatus`, `toBeApiError`, `toMatchContract`, `toMatchEventSchema`, `toMatchSchema`. Failure messages show the response body, URL and correlation ID.

## 4. Test environment and data

- **Two ways to run the stack.**
  - With `LOYALTY_URL` unset, Playwright starts the four services itself, on the in-memory broker. This is the fast local loop.
  - With `LOYALTY_URL` set, it uses a stack that is already running. This is how CI uses Docker Compose, and how the suite would run against a shared test environment.
- **Two message buses.** `docker-compose.yml` runs on the in-memory broker. Adding `docker-compose.kafka.yml` runs a single-node Kafka (KRaft) and a one-shot job that creates the topics: `loyalty.events`, `profile.events`, and for each consumer group a dead-letter topic and three retry topics, each with 3 partitions. `KAFKA_RETRY_MODE` (`blocking` or `topics`) chooses how the consumers retry (§7.11). The CDP keeps profiles, seen event IDs and its outbox in SQLite on a Docker volume. A second CDP instance (`customer-data-platform-2`, Compose profile `scale`) shares that file and joins the same consumer group, for the rebalance test. Consumers use static group membership (`CONSUMER_INSTANCE_ID`), so a restarted instance gets its partitions back at once. The chaos tests drive Docker Compose from the test process. On Kafka the services produce and consume directly, and the broker service becomes the tests' inspection API over Kafka, with the same HTTP endpoints. The tests run with `BUS=kafka` to add the Kafka project.
- **Isolation by data, not by environment.** Every test creates its own customer with a unique ID (`qa-<time>-<random>`), so tests run fully in parallel against shared services and need no cleanup. Consumer tests that publish to the broker directly use synthetic `evt-` customers that exist only in the event stream.
- **Traceability.** Every test sends its own `X-Correlation-ID`. It flows through the engine, the events, the broker and the notification. On failure, the correlation ID and customer IDs are printed and attached to the report, so the run can be found in the service logs.
- **Test-only endpoints** are kept apart from the public API: `/internal/outbox` (published events) and `/internal/faults` (fault injection), marked as test-only in the specs. They are contract-tested like every other endpoint, and so are the notification decisions. The broker is test infrastructure with no spec, so its inspection endpoints are not contract-checked.

## 5. Entry and exit criteria

### Entry criteria (a build is ready for testing)

1. The code compiles (`npm run typecheck`).
2. The stack starts, and every service answers `GET /health` (Compose waits for the health checks).
3. The specs in `specs/` are updated for any API or event change in the build.
4. The contract project passes. If it does not, the behaviour tests are skipped, and the build goes back with the contract failure as the only reported problem.

### Exit criteria (a change may be merged or released)

1. All tests pass on the pull request and on `main`, in all three configurations, with **zero retries** (§7.3).
2. No test is skipped, except by the coverage guards on purpose. CI uses `forbidOnly`, so a stray `test.only` fails the run.
3. The nightly run, which repeats every test 3 times, has been green since the change was merged. A failure there is treated as a defect (a flaky test or a race in the system), not ignored.
4. Every new endpoint, response status, event type or field mapping has its row in a spec, and the coverage tests prove it is exercised. They fail when a documented response or a mapping row has no test.
5. Any defect found is either fixed with a regression test, or recorded with an accepted risk.

## 6. Acceptance criteria

Each criterion below is automated; the test that checks it is named in brackets.

### Earn, redeem, refund

- **Given** a new customer, **when** they earn 250 points, **then** the response is 201, the balance is 250, and the ledger has one EARN entry of +250 with `balanceAfter` 250. *[earn: "earning adds points and writes one EARN ledger entry"]*
- **Given** a balance of 300, **when** the customer redeems 301, **then** the response is 422 `INSUFFICIENT_BALANCE` and the ledger is unchanged. *[redeem: "redeeming balance + 1 is rejected and the ledger is unchanged"]*
- **Given** an earn of 100 already refunded as 60 + 40, **when** 1 more point is refunded, **then** it is rejected with 422 `REFUND_EXCEEDS_ORIGINAL`. *[refund: "partial refunds can add up to the original, but not past it"]*
- **Given** points from an earn that have already been redeemed, **when** that earn is refunded, **then** the refund is rejected with 422 `INSUFFICIENT_BALANCE`, so the balance never goes negative. *[refund: "a refund is rejected when the points were already redeemed"]*
- **Given** points that are 0, negative, a decimal, a string, null or a boolean, **when** the earn is sent, **then** it is rejected with 400 `VALIDATION_ERROR` naming the `points` field, and nothing is written. A server-owned field such as `balance` in the body is rejected the same way. *[earn: "points that is … is rejected and nothing is written", "a client cannot set server-owned fields such as balance"]*

### Tiers

- **Given** 999 qualifying points, **when** the customer earns 1 more, **then** the tier is Silver. Gold starts at exactly 5,000. *[tiers: "crossing a threshold by one point upgrades the tier", the boundary table]*
- **Given** a Silver customer, **when** they redeem points, **then** the tier stays Silver. **When** a refund takes them below 1,000 qualifying points, **then** the tier drops to Bronze. *[redeem: "redeeming does not lower the tier"; refund: "a refund can move the customer down a tier"]*

### Idempotency

- **Given** a request with an `Idempotency-Key`, **when** the same request is sent again with that key, **then** the second response is identical and marked `Idempotent-Replayed: true`, and the ledger has one entry. *[idempotency: "the same request sent twice with one key writes one ledger entry"]*
- **Given** 10 identical requests with one key sent in parallel, **then** exactly one entry is written and all 10 responses are identical. *[idempotency: "ten parallel requests with one key produce one entry and identical responses"]*
- **Given** a used key, **when** it is reused with a different body, endpoint or customer, **then** the response is 409 `IDEMPOTENCY_KEY_REUSED`. *[idempotency: the three "reusing a key …" tests]*

### Events and profile sync

- **Given** a committed write, **then** the engine publishes one event per ledger entry, plus `tier.changed` directly after it when the tier moves. Every event matches `asyncapi.yaml`, sequences run 1..n per customer, and a rejected request publishes nothing. *[events.contract]*
- **Given** a new customer with consent, **then** within 3 seconds the CDP profile exists with tier `bronze`, balance 0 and `marketing_consent` true. *[profile-sync]*
- **Given** the same event delivered twice, **then** it is applied once, and both copies are acknowledged so the broker stops resending. *[duplicates]*
- **Given** a customer's events delivered in reverse order, **then** the profile ends up the same as with in-order delivery, and an older event never overwrites a newer value. *[ordering]*
- **Given** a consumer that fails temporarily, **then** the broker retries with growing waits and delivers once the consumer recovers. **Given** a message that can never be accepted (invalid) or that keeps failing (poison), **then** it goes to the dead-letter queue: invalid ones after one attempt, poison ones after 4. It does not block later messages, and can be redriven once the cause is fixed. *[dead-letter]*

### Data integrity

- **Given** any history of accepted and rejected writes for a customer, **then**:
  - the ledger entries sum to the balance;
  - every `balanceAfter` equals the running total, and none is negative;
  - transaction IDs are unique;
  - refunds point to an earlier EARN and never exceed it;
  - balance, qualifying points and tier from the API all agree with the ledger;
  - every entry was published exactly once with the same values, and nothing else was published.

  *[ledger-reconciliation: the seeded runs and parallel runs]*
- **Given** later writes, **then** earlier ledger entries never change (append-only). *[ledger-reconciliation]*
- **Given** each row of the mapping table in `cdp-openapi.yaml`, **then** the profile attribute equals the value transformed from the event the engine actually published, for example `tier "Gold"` → `loyalty_tier "gold"`. Every profile attribute has a row, and every row has a test. *[field-mapping]*

### Notifications

- **Given** a customer with marketing consent, **when** an earn moves them from Bronze to Silver, **then** exactly one `tier-upgrade` notification is recorded, carrying the transaction ID and the request's correlation ID. *[tier-notifications]*
- **Given** no consent, or consent that is unknown because the tier change arrived before `customer.created`, **then** no notification is recorded, and the decision says why. *[tier-notifications, notification-reliability]*
- **Given** a write that is rejected (insufficient balance, a validation error, a refund that is too large, an unknown transaction), **then** no event is published and no notification is recorded. *[tier-notifications: "failed transactions send nothing"]*
- **Given** at-least-once delivery, **then** a tier change still produces exactly one notification. This holds when the engine event is duplicated, when the acknowledgement is lost at the CDP or at the notification service, and when the profile event is republished under a new ID. *[notification-reliability]*
- **Given** Bronze → Gold in one earn, or 10 parallel earns that cross a threshold together, **then** one notification is recorded, not several. *[tier-notifications]*
- **Given** an upgrade that arrives after a newer downgrade, **then** only the downgrade is notified. The profile already shows the current tier, so the stale event triggers nothing. *[notification-reliability]*

## 7. Design trade-offs

### 7.1 Polling instead of sleeping

Asynchronous checks use `expect.poll`, which checks every 25–500 ms until the expected state appears or a time limit passes: 3 s for one hop, 6 s for engine → notification. A fixed `sleep` is either too short, making tests flaky on a slow CI runner, or too long, making the suite slow everywhere. Polling is as fast as the system allows and fails with the last value seen.

Proving that something did *not* happen is harder, because waiting for nothing is just a sleep in disguise. The suite avoids that in two ways:

- **Decision records.** The notification service records a decision for every event, including suppressed and duplicate ones. A test waits for the decision, then asserts that there is no notification.
- **Markers.** Where nothing should be published at all, a later write that does produce an event acts as a marker. Once the marker has arrived, anything the earlier writes might have caused would have arrived too.

### 7.2 Contract tests run first

The `api`, `events`, `integrity` and `notifications` projects depend on `contract`. If a spec, the validator or a response shape is broken, every behaviour test would fail for the same root cause, and the report would show hundreds of failures. Skipping them reports one clear contract failure instead. When this happens, Playwright shows the skipped tests as "did not run".

### 7.3 No automatic retries

`retries: 0`. A test that passes on retry is a flaky test, and in an event-driven system flakiness is often a real race condition. Retries would hide it. The nightly run repeats every test 3 times to bring such problems out.

### 7.4 Mock services, real behaviour

The services are small, but they implement the behaviour that makes integrations hard:

- at-least-once delivery with no ordering guarantee;
- retries with backoff and dead-letter queues;
- per-customer sequence numbers;
- an outbox relay, so a broker outage delays events instead of losing them or failing the write;
- a 50 ms processing delay in the engine, so parallel requests really overlap and the concurrency tests can fail.

The trade-off is that a mock can only have the failure modes someone thought to build into it. Against real systems, the same tests would run unchanged, as the `LOYALTY_URL` mode shows. The fault-injection tests would need the real systems' own ways to cause failures.

### 7.5 Two buses: an in-memory broker and Kafka

The in-memory broker pushes messages over HTTP. It starts instantly, needs no Docker, and keeps the local loop fast. On Kafka, the services produce and consume records themselves. The two keep the same delivery rules: at least once, retries with backoff, and a dead-letter destination.

The tests never talk to Kafka directly. They use the broker service's HTTP API through `QueueInspector`, and on Kafka that service is a view over the topics:
- it produces records the way a service would;
- it indexes every topic and dead-letter topic;
- it asks each consumer what it did with a record.

Its reads catch up to the topics' end offsets before answering, so a record a service produced just before the request is included. That design was tested for real when the first 320 tests passed on Kafka without a single change. The cost is that the inspection API is code of its own, and could hide a Kafka problem behind a bug of its own. The Kafka project checks Kafka's own properties against the records' real partitions and offsets.

A RabbitMQ variant would plug in the same way. It was skipped in favour of Kafka, which is the bus this kind of integration usually runs on.

### 7.6 Hand-written validation in the services

The services do not generate their validation from the specs, just as real services often don't. That makes drift possible, and the parity tests exist to catch it: the same payloads go to the service and to the schema, and both must give the same verdict. Generating the validation would remove the drift, and with it the reason for those tests.

### 7.7 Field-level versioning in the CDP

The CDP keeps a sequence version for each group of fields (tier, balance, membership, activity), instead of applying "last event wins". This is what makes out-of-order delivery converge to the right state. The cost is that a late event can partly apply: it can update the activity timestamp but not the tier. The tests pin down exactly which fields such an event may change.

### 7.8 Unknown consent counts as no consent

If a tier change reaches the CDP before `customer.created`, consent is not yet known. The notification service then suppresses the message and does not send it later, even after consent arrives. Missing one welcome-to-Silver message is a smaller risk than messaging someone who did not agree to it. This is a product decision, and the tests document it so that changing it is a deliberate act.

### 7.9 Downgrades are notified too

A downgrade sends a `tier-downgrade` notification. The README only requires upgrades. Notifying both makes "exactly one per tier change" a single rule, and a downgrade is the change a customer most wants explained. Limiting it to upgrades would be a one-line change in the service and its rules table.

### 7.10 Fixed seeds for random tests

The random ledger runs use three fixed seeds instead of a new random seed each time. A fixed seed always replays the same run, so a failure can be reproduced; a new seed each run explores more histories but can fail without a way to replay it. Adding a nightly job with a random seed, printed in the report, would combine both.

### 7.11 Blocking retries or retry topics, and the noisy-neighbour effect

The Kafka consumers have two retry modes, chosen with `KAFKA_RETRY_MODE`:

- **Blocking (the default).** A failing record is retried in place, so its partition waits until the record is acknowledged or dead-lettered. Each customer's events stay in order, because a later event can never overtake a retried one. The cost is head-of-line blocking: every other customer on the same partition waits too.
- **Retry topics.** A failing record is moved to `<group>.retry-1`, then `retry-2`, `retry-3` (100, 200, 400 ms) and finally the dead-letter topic. The source offset is committed as soon as the record is parked, so the partition keeps moving. Each retry topic has one fixed delay, so its records become due in the order they arrive, and the consumer waits only on that retry partition. The cost is order: a customer's later event can overtake a retried one, so the consumer must cope with reordering. The CDP can, through sequence versioning (§7.7). A test shows sequence 3 applied before sequence 2, and the profile still ending with the right balance.

Blocking stays the default because it is simpler to reason about and needs no extra topics. Retry topics are the better choice when one slow customer must not delay the others, as long as every consumer tolerates reordering. CI runs the whole suite in both modes, plus the tests specific to the running mode. The general tests pass unchanged in both.

The tests ran into the blocking mode's cost themselves. The head-of-line test failed 2 times in 6 when parallel copies of it, or other tests with injected faults, blocked the "other" partition too; with 3 partitions, a few parallel faults cover them all. Run alone, it passed every time. So the `kafka` project runs after all the others, with one worker. A side effect: Playwright does not repeat projects that another project depends on, so the nightly Kafka run is split into two runs to repeat everything.

### 7.12 Recovery: what is persisted, and what the chaos tests found

**Persisted.** On Kafka, the CDP writes one SQLite transaction per event: the seen-event row (deduplication), the profile, and the outgoing profile event (outbox). A crash leaves either all of it or none.
- The chaos test crashes the process right after that transaction, before the offset is committed. After Docker restarts it, the record is read again, recognised as a duplicate, and the outbox row is sent once: one notification.
- Bug injection showed both halves are needed. Without the persisted seen-events the event is applied twice; without the persisted outbox the notification is lost.

SQLite on a shared volume, rather than a database server, keeps the stack small. Two instances can share it on one host, which is enough for the rebalance test. `node:sqlite` is still experimental in Node 24, which is acceptable for a mock.

**Not persisted.** The engine's ledger, its relay queue and the notification service are in memory. During a Kafka outage the engine's relay holds the events and sends them once Kafka is back; the test shows the backlog growing and draining. But if the engine itself crashed during the outage, those events would be lost. Persisting the engine's outbox is the next step (§9).

**What the chaos tests found.** Two real defects, both fixed:
1. **A consumer that silently stopped.** After a long Kafka outage the client library gave up fetching and closed the consumer's stream. The consumer stayed in the group with no partitions, and `/health` still said ready; nothing was read again. The fix is a supervisor that restarts a failed stream with a new consumer, and readiness that reflects "joined and reading". The outage test now keeps Kafka down until the CDP reports not ready, then checks it recovers. Without the supervisor, it fails.
2. **A retry 1 ms too early.** `setTimeout` can fire a millisecond before `Date.now()` reaches the due time, so a retry could start just before its backoff had passed. The timing test caught a gap of 199 ms where at least 200 was required, once in about 45 runs. Waits now loop until the due time has passed.

A third finding was in the tests themselves. The broker's `/queues` counted dead letters by asking a consumer about each one. It slowed down as the dead-letter topic grew, until a test that samples "committed" and then "still pending" could straddle the moment the record was delivered. The count is now taken from the index, and the tests read both values in one sample.

**A limit worth knowing.** The Kafka client's `send` does not fail while Kafka is unreachable; it waits. So the relay never sees an error during an outage, and a "drop on error" bug in it would go unnoticed by any test. That is why the outage test checks the outcome (events held, then delivered) rather than the error path.

## 8. Reporting and defect handling

- **Reports.** Every CI run uploads the Playwright HTML report as an artifact, one per bus. A failed run also uploads the raw test results and the logs of all four containers. In CI, failures are also annotated on the commit, and a JUnit file is written for tools that read one.
- **Failure context.** Each failure shows the correlation ID and customer IDs, the response body and URL, and, for schema failures, every violated rule with its path. That is usually enough to find the run in the service logs without rerunning it.
- **Defects.** Each defect gets a regression test that fails before the fix. When a test turns out to be wrong instead, it is fixed and the cause is recorded in the commit message. An example from step 4: Playwright JSON-encodes a string body that is not valid JSON, so a "malformed JSON" test had been sending valid JSON.

## 9. Known gaps and next steps

- **A Kafka cluster**: three brokers with replication, to test broker failover and `min.insync.replicas`. Plus a schema registry for message contracts.
- **Automated mutation testing** (for example Stryker) on the services, in place of the manual runs described in §3.
- **Consumer-driven contracts** between the real teams, so a provider change is checked against what each consumer actually uses.
- **A consent-change event**, and tests for consent withdrawn between a tier change and its notification.
- **Persist the engine's outbox** (and the notification service's state), then test an engine crash during a Kafka outage.
- **Load tests** with latency targets, kept separate from the functional suite.

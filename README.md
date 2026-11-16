# Loyalty Integration QA

An API and event-driven test suite for a loyalty platform: points, customer tiers, a transaction ledger, customer profile sync and notification triggers.

The system under test is a small set of mock services that copy the shape of a real loyalty stack (a promotion/loyalty engine, a customer data platform and a messaging service). The point of the project is the tests, not the services.

## What is under test

```
 ┌──────────────┐  REST   ┌────────────────┐  event   ┌──────────────┐  event   ┌──────────────┐
 │  Test suite  │ ──────► │ Loyalty engine │ ───────► │ Customer data│ ───────► │ Notification │
 │ (Playwright) │         │ points, tiers, │  queue   │ platform     │  queue   │ service      │
 └──────────────┘         │ ledger         │          │ profile sync │          │ email / push │
                          └────────────────┘          └──────────────┘          └──────────────┘
```

1. A client calls the **Loyalty engine** over REST (earn, redeem, refund, tier lookup).
2. The engine writes to its ledger and publishes an event (`points.earned`, `tier.changed`, ...).
3. The **Customer data platform** consumes the event and updates the customer profile.
4. On a tier change, the **Notification service** sends a message (recorded, not delivered).

## What the tests cover

| Area | Examples |
|---|---|
| REST API | Earn, redeem, refund, tier lookup; status codes; error bodies |
| Negative and boundary | Redeem more than the balance, zero and negative amounts, tier thresholds ±1 point, unknown customer |
| Schema validation | Every response and every event checked against OpenAPI / JSON Schema |
| Async data flow | Event reaches the profile within a time limit (polling, no fixed sleeps) |
| Event reliability | Duplicate events, out-of-order events, retries, dead-letter queue |
| Idempotency | Same request sent twice with one idempotency key → one ledger entry |
| Data integrity | Field mapping between systems; ledger sum equals balance |
| Notifications | Tier upgrade triggers exactly one message; no message on a failed transaction |

## Tech stack

- **Playwright + TypeScript** with `APIRequestContext` (API testing only, no browser)
- **Ajv** for JSON Schema validation, with schemas taken from the OpenAPI spec
- **Node.js mock services** on two message buses: an in-memory queue, and Kafka through Docker Compose
- **Docker Compose** to run the whole stack locally and in CI
- **GitHub Actions** for runs on every push plus a nightly run, with an HTML report published as an artifact

## Project layout

```
services/        mock loyalty engine, customer data platform, notification service
specs/           OpenAPI spec and event schemas (single source of truth)
tests/
  api/           REST: functional, negative, boundary
  events/        async flow, duplicates, ordering, dead-letter
  integrity/     field mapping, ledger reconciliation
  notifications/ notification triggers
src/
  clients/       typed API clients (service objects)
  builders/      test data builders (customers, transactions)
  helpers/       polling, schema validator, queue inspector
docs/
  TEST_STRATEGY.md
```

## Run it

```bash
docker compose up -d
npm ci
npx playwright test
npx playwright show-report
```

## Test strategy

`docs/TEST_STRATEGY.md` covers scope, risks, entry and exit criteria, acceptance criteria per feature, and the trade-offs behind the design, such as why the tests poll instead of sleeping and why contract checks run before end-to-end tests.

## Status

Complete: REST suite → schema validation → async event tests → integrity checks and notifications → CI → test strategy. 320 tests run on every push to `main`, on pull requests and nightly in GitHub Actions. Planned next steps are listed in `docs/TEST_STRATEGY.md` (section 9).

# Project context for Claude Code

## Goal
Portfolio repo for a Senior QA Lead application (Vancouver contract role). The role tests integrations between a loyalty/promotion engine, a customer data platform and downstream loyalty notification systems. Its key requirements: Playwright + TypeScript API automation; REST, JSON payloads, schema validation and integration testing; asynchronous and event-driven systems; data integrity, field mapping and synchronization testing; CI/CD.

The repo should show those skills directly. The tests are the product. The services are small mocks.

## System under test (mocked, Node.js + TypeScript)
- **loyalty-engine**: REST API for earn, redeem, refund and tier lookup. Append-only points ledger. Tiers: Bronze 0–999, Silver 1000–4999, Gold 5000+. Supports the `Idempotency-Key` header. Publishes `points.earned`, `points.redeemed` and `tier.changed` events.
- **customer-data-platform**: consumes events, deduplicates by `eventId`, maps fields into the profile (`tier "Gold"` → `attributes.loyalty_tier "gold"`), exposes `GET /profiles/{id}`.
- **notification-service**: on `tier.changed`, records one notification (not really sent). Respects a consent flag.
- **Broker**: start with an in-memory queue. RabbitMQ through Docker Compose is optional later. Needs a dead-letter queue for malformed or poison messages.

## Test areas (see README table)
REST functional/negative/boundary · schema validation (Ajv, schemas in `specs/`, from OpenAPI + AsyncAPI) · async flow with `expect.poll` (no fixed sleeps) · duplicate and out-of-order events · retries and DLQ · idempotency (including parallel same-key requests) · field mapping per row · ledger reconciliation (sum of entries = balance) · notification triggers (exactly one; none without consent; none on failed transactions).

## Conventions
- Playwright `APIRequestContext` only, no browser.
- Service-object clients in `src/clients/`, test data builders in `src/builders/`, helpers (polling, schema validator, queue inspector) in `src/helpers/`.
- Each test creates its own customer with a unique ID and sets its own `X-Correlation-ID`. Failure messages print both.
- Money and points are integers. Timestamps are ISO 8601 UTC.
- `docs/TEST_STRATEGY.md`: scope, risks, entry/exit criteria, acceptance criteria (Given/When/Then), design trade-offs.

## Build order
1. Repo + README (done in chat; README.md here is final, do not rewrite).
2. Mock loyalty-engine + REST suite.
3. Schemas + validation.
4. Event bus + CDP + async tests.
5. Integrity checks + notifications.
6. Docker Compose + GitHub Actions (push + nightly, HTML report as artifact).
7. TEST_STRATEGY.md.

Commit after each step with clear messages.

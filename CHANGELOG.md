# Changelog

All notable changes of `csp-worker` are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the versions follow
[Semantic Versioning](https://semver.org/). A release is a `release/<version>` branch cut from `main` and filled with the commits of `qa`,
re-applied with `git cherry-pick -x` (numerals 6.2.3, 10 and 11 of the course norm); it reaches `main` by pull request, never by merging `qa`,
and is tagged `v<version>` once it is merged.

## [Unreleased]

### Added

- Dispatch of the ticket emails (HU-TICKETING-002, ADR-057): `dispatch-ticket-emails` calls `POST /internal/fulfillments/dispatch` on the ticketing service in batches (1 to 100, 50 by default) until a batch comes back short, or the run is out of time or calls, with the service token and the correlation id of the run. The service sends the emails and keeps a delivery pending, with a growing delay, while the contact of the user is not known. Off by default (`DISPATCH_TICKET_EMAILS_ENABLED=false`, `DISPATCH_TICKET_EMAILS_EVERY`, `DISPATCH_TICKET_EMAILS_BATCH_SIZE`, `DISPATCH_TICKET_EMAILS_RUN_TIMEOUT`, `TICKETING_API_URL`).
- Outbox relay of Ticketing (HU-TICKETING-002, ADR-057): `ticketing-outbox-relay` publishes the `TicketIssued` events of `ticketing_fulfillment.outbox_event` to `cine.events` with the same at-least-once delivery, envelope check and backoff as the other relays, with routing key `cine.ticketing_fulfillment.ticket-issued.v1`, its own state key, lock and connection. Off by default (`TICKETING_OUTBOX_RELAY_ENABLED=false`, `TICKETING_OUTBOX_RELAY_EVERY`, `TICKETING_OUTBOX_RELAY_EVENT_TYPES`).

### Fixed

- `develop` did not compile (HU-TICKETING-002): `GenerateTicketPdfJob` declared `name()` twice and its configuration used a `RelayDomain.TICKETING` that does not exist. The job was a draft (it built the ticket from random values and published again the Booking event it read) and contradicted ADR-057, which makes the worker ask `csp-ticketing-api` which tickets lack a PDF. The job, its configuration, its `generate-ticket-pdf` settings, the `TicketingApi` and `ObjectStoragePort` ports, their two adapters and the unused starters are removed. The PDF generator port and adapter stay for the real job.
- The outbox relays no longer declare the exchange (HU-AUTH-004, ADR-036): the login `worker_relay` has `configure ^$`, so the declaration was refused and nothing was published. `AmqpEventPublisher` now checks that the exchange exists with a passive declaration and publishes; a missing exchange is a retryable failure that logs its name (no credential), reopens the connection and recovers when the exchange appears. An AMQP URL with an empty virtual host (ending in a bare `/`) stops the startup with a message that asks for `/%2F`.

### Added

- Outbox relay of Concessions (HU-CONCESSIONS-004, ADR-019, ADR-045): `concessions-outbox-relay` publishes `concessions.outbox_event` to `cine.events` with the same at-least-once, backoff and permanent-failure rules, its own state key, lock and connection, and only the event types of `CONCESSIONS_OUTBOX_RELAY_EVENT_TYPES` (`ConcessionOrderConfirmed` by default, the one Ticketing consumes). The routing key drops the word "Concession" of the event type (`cine.concessions.order-confirmed.v1`). Off by default (`CONCESSIONS_OUTBOX_RELAY_ENABLED`).
- `expire-concessions-orders` job (HU-CONCESSIONS-004, ADR-045): calls `POST /internal/orders/expire` on the concessions service in batches until it reports no more, or the run is out of time or calls, to release the orders without a reservation whose time ran out; off by default (`EXPIRE_CONCESSIONS_ORDERS_ENABLED`). The loop that drains a service in batches is now `DrainLoop`, shared with `purge-auth`, whose behavior does not change.
- `concessions-reservation-events` job composed (HU-CONCESSIONS-004, ADR-045): `AmqpReservationEvents` declares the queues `cine.concessions.booking-confirmed` and `cine.concessions.reservation-expired` bound to `cine.events` with a dead-letter queue each, using the broker login of Concessions (ADR-036), and gives the job one message at a time; the job runs every `CONCESSIONS_EVENTS_EVERY` seconds when `CONCESSIONS_EVENTS_ENABLED` is on (off by default) and needs `CONCESSIONS_AMQP_URL`, `CONCESSIONS_API_URL` and the service token. End-to-end test from the broker to a stand-in of the service.
- Reservation events job and Concessions client (HU-CONCESSIONS-004, ADR-045): `ReservationEventsJob` settles the concession order of a reservation from the Booking events `BookingConfirmed` and `ReservationExpired` of a queue port, acknowledging a message only after the service accepted it, sending a refused one to the dead-letter queue and leaving the message in the queue when the service is down; `ConcessionsOrdersClient` calls `POST /internal/orders/by-reservation/{reservationId}/confirm` and `.../release` with the service token and the correlation id of the run. `RetryingHttp` now accepts any `2xx` and reports the status of a failure. Not composed yet: the queue adapter and the flag come in the next pull request.
- `deploy/compose.yml` passes `AUTH_OUTBOX_RELAY_ENABLED` (off), `AUTH_OUTBOX_RELAY_EVERY` and `AUTH_OUTBOX_RELAY_EVENT_TYPES` with the defaults of the application (code-corhuila/csp-worker#30).
- Outbox relay of auth (ADR-019): `auth-outbox-relay` publishes `auth.outbox_event` to `cine.events` with the same at-least-once, backoff and
  permanent-failure semantics as the booking relay, with its own state key (`source` `auth`), advisory lock and broker connection. Off by default
  (`AUTH_OUTBOX_RELAY_ENABLED`); it publishes only the event types of `AUTH_OUTBOX_RELAY_EVENT_TYPES` (`UserRegistered,UserEmailChanged` by default), so
  `UserRoleChanged` can be enabled later with no code change. Routing keys `cine.auth.<event-type>.v1`. Must not be turned on before the consumers exist.
- Instance lock of the outbox relay (ADR-014, code-corhuila/csp-worker#26): a run takes a PostgreSQL session advisory lock before it
  publishes, and an instance that does not get it skips the run, so two relays never publish at the same time. The lock port is `RunLock`,
  the adapter `JdbcAdvisoryRunLock` (a connection of its own, released when it closes or dies). The README no longer describes the single
  instance as a convention.
- `purge-auth` job (HU-AUTH-002, ADR-030): on a schedule it calls the internal purge of `csp-auth-api` in batches until the
  service reports no more, the run is out of time or it reaches its cap of calls, with the service token. Off by default
  (`PURGE_AUTH_ENABLED`); settings `AUTH_API_URL`, `PURGE_EVERY`, `PURGE_BATCH_SIZE` and `PURGE_RUN_TIMEOUT`.

### Changed

- The end-to-end test of the auth relay no longer assumes the publication order of the events of one batch, which the relay does not guarantee.
- The relay adapters take the domain they serve (`RelayDomain`): schema of the outbox, `source` of the state, lock key, routing key domain and the
  envelope `source`. The booking relay behaves as before.
- The retry policy of the HTTP clients moved to a shared `RetryingHttp` with the same behaviour; `ServiceTokenSource`
  exists while `expire-holds` or `purge-auth` is on.

## [2.0.0] - 2026-10-08

First release. Background jobs of the Booking domain (numeral 4.3), serving the stories HU-BOOKING-001, HU-BOOKING-002 and
HU-BOOKING-003 of [csp-booking-api](https://github.com/code-corhuila/csp-booking-api).

### Added

- Three-module Maven project (`worker-core`, `worker-adapters`, `worker-app`) that builds an executable jar and a container image
  that `csp-infra-postgres` composes. ([#1](https://github.com/code-corhuila/csp-worker/pull/1),
  [#3](https://github.com/code-corhuila/csp-worker/pull/3))
- `expire-holds` job: on a schedule it calls the internal sweep of `csp-booking-api` with a service token, so overdue holds expire
  and their seats are released (DEC-004). ([#2](https://github.com/code-corhuila/csp-worker/pull/2))
- Outbox relay of Booking (ADR-014): it reads `booking.outbox_event` with a `SELECT` only, keeps its state in the `worker` schema
  and publishes the events to RabbitMQ with publisher confirms, so a consumer deduplicates by `eventId`.
  ([#7](https://github.com/code-corhuila/csp-worker/pull/7), [#8](https://github.com/code-corhuila/csp-worker/pull/8),
  [#9](https://github.com/code-corhuila/csp-worker/pull/9))
- Scheduler with one flag and one thread per job (`EXPIRE_HOLDS_ENABLED`, `OUTBOX_RELAY_ENABLED`), so a slow job never delays the
  others, and the compose wiring with the platform. ([#10](https://github.com/code-corhuila/csp-worker/pull/10))

### Known limits

- The relay is off by default (`OUTBOX_RELAY_ENABLED=false`) and must run as a single instance per environment: two instances do
  not coordinate and could publish the same event twice.
- The exchange `cine.events` is an assumption until the event catalogue names it, and the consumers must declare their queues
  before the relay is turned on.
- A run of the relay does not stop at the first transient failure of the broker.
- No readiness probe, metrics or alerts of ADR-014.
- No heavy job (such as the PDF of the ticket) is implemented; where it runs is still to be decided with Ticketing.

# Changelog

All notable changes of `csp-worker` are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the versions follow
[Semantic Versioning](https://semver.org/). A release is a `release/<version>` branch cut from `main` and filled with the commits of `qa`,
re-applied with `git cherry-pick -x` (numerals 6.2.3, 10 and 11 of the course norm); it reaches `main` by pull request, never by merging `qa`,
and is tagged `v<version>` once it is merged.

## [Unreleased]

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

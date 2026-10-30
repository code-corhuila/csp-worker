# csp-worker

Scheduled background jobs for the CineSync Platform (Norma 4.3). The worker runs jobs that no one
asks for: expiration sweeps, notifications, reindexation. It exposes no business interface
(Norma 5.7.1 admits at most a health endpoint, which is not implemented yet: code-corhuila/csp-worker#18).

## Jobs

| Job | Schedule | Description |
|---|---|---|
| `expire-holds` | Every 60s | Calls `POST /internal/maintenance/expire-holds` on the booking service to expire HELD reservations past their hold time (HU-BOOKING-002) |
| `purge-auth` | Every 3600s, when enabled | Calls `POST /internal/maintenance/purge` on the identity service in batches of up to 500 rows until it reports no more, or the run is out of time (60s) or calls (100) (HU-AUTH-002, [ADR-030](https://github.com/code-corhuila/csp-docs/blob/main/05-architecture/decisions/records/ADR-030-auth-retention-and-purge.md)) |
| `outbox-relay` | Every 5s, when enabled | Publishes the events the booking API wrote to `booking.outbox_event` to RabbitMQ, with at-least-once delivery ([ADR-014](https://github.com/code-corhuila/csp-docs/blob/main/05-architecture/decisions/records/ADR-014-booking-outbox-relay-read-only.md)) |
| `auth-outbox-relay` | Every 5s, when enabled | Publishes the events of `auth.outbox_event` that have a consumer to RabbitMQ, with the same delivery semantics ([ADR-019](https://github.com/code-corhuila/csp-docs/blob/main/05-architecture/decisions/records/ADR-019-outbox-relay-read-only-for-other-domains.md)) |
| `concessions-outbox-relay` | Every 5s, when enabled | Publishes the events of `concessions.outbox_event` that have a consumer to RabbitMQ, with the same delivery semantics ([ADR-019](https://github.com/code-corhuila/csp-docs/blob/main/05-architecture/decisions/records/ADR-019-outbox-relay-read-only-for-other-domains.md), [ADR-045](https://github.com/code-corhuila/csp-docs/blob/main/05-architecture/decisions/records/ADR-045-concessions-orders-stock-holds-and-reservation-events.md)) |
| `ticketing-outbox-relay` | Every 5s, when enabled | Publishes the `TicketIssued` events of `ticketing_fulfillment.outbox_event` to RabbitMQ, with the same delivery semantics ([ADR-057](https://github.com/code-corhuila/csp-docs/blob/main/05-architecture/decisions/records/ADR-057-ticket-issuance-pdf-in-worker-and-delivery-retry.md)) |
| `concessions-reservation-events` | Every 5s, when enabled | Takes `BookingConfirmed` and `ReservationExpired` from the queues of Concessions and calls `POST /internal/orders/by-reservation/{reservationId}/confirm` or `.../release` on the concessions service, so the order of a reservation is confirmed or released when the reservation is (HU-CONCESSIONS-004, [ADR-045](https://github.com/code-corhuila/csp-docs/blob/main/05-architecture/decisions/records/ADR-045-concessions-orders-stock-holds-and-reservation-events.md)) |
| `expire-concessions-orders` | Every 60s, when enabled | Calls `POST /internal/orders/expire` on the concessions service in batches of up to 100 orders until it reports no more, or the run is out of time (60s) or calls (100): it releases the orders without a reservation whose `ORDER_HOLD_MINUTES` ran out (HU-CONCESSIONS-004, [ADR-045](https://github.com/code-corhuila/csp-docs/blob/main/05-architecture/decisions/records/ADR-045-concessions-orders-stock-holds-and-reservation-events.md)) |

### Outbox relay

Off by default. It reads the booking outbox with `SELECT` only, through the `worker_app` login, and keeps what it
has published in schema `worker` (`outbox_relay_state`, `outbox_relay_cursor`); it never writes the booking schema.
A run publishes at most 100 events in 20 seconds. A failed publication is retried with exponential backoff (1 second
up to 5 minutes, 20 attempts); an invalid envelope or a message no queue is bound to fails for good and is replayed by
an operator setting `status = 'PENDING'` and `attempt_count = 0` in `worker.outbox_relay_state`.

To enable it: create the schema with the migration of `db/` (`docker compose --profile tooling run --rm
worker-db-migrate`, as the instance administrator), point `RELAY_AMQP_URL` at the broker and set
`OUTBOX_RELAY_ENABLED=true`. Consumers deduplicate by `eventId`, the `messageId` of every message.

The relay runs as **one instance per environment** (ADR-014), and the worker enforces it: each run first takes a PostgreSQL
session advisory lock on a connection of its own, and an instance that does not get it does nothing and logs one line at `DEBUG`;
it tries again on its next run, so it takes over when the holder stops. The lock is held for the run, which is bounded (100 events,
20 seconds, Norma 5.7), including the calls to the broker. PostgreSQL gives the lock back when the connection closes or dies, so
a crashed instance never blocks the others. Turning the flag on in more than one instance is therefore safe, but the other
instances are idle: do not use it to scale the relay.

Before enabling it in a shared environment, the consumers must have declared their queues: a message that no queue is bound
to is a permanent failure and waits for an operator. The exchange is `cine.events`, fixed by ADR-036 (`csp-infra-rabbitmq`).
The relay never declares it: its login (`worker_relay`) may only write to it, so the publisher checks that the exchange exists
(a passive declaration, which needs no permission) and a missing one is a retryable failure, logged with the exchange name, that
the relay recovers from by itself once the broker has it.

### Auth outbox relay

Same relay, reading `auth.outbox_event` instead of the booking outbox ([ADR-019](https://github.com/code-corhuila/csp-docs/blob/main/05-architecture/decisions/records/ADR-019-outbox-relay-read-only-for-other-domains.md)): `SELECT` only, through the same `worker_app` login
(role `auth_outbox_reader`, granted by `csp-auth-db`), and the same publication state in schema `worker`, kept apart by its `source` (`auth`) so booking and
auth rows never collide. Delivery is at-least-once, with the same envelope check, backoff and permanent failures; an unroutable message is a permanent failure.
It has its own flag, its own PostgreSQL advisory lock and its own broker connection, so it never waits for the booking relay.

- **Off by default** (`AUTH_OUTBOX_RELAY_ENABLED=false`). **Do not turn it on before the consumers exist**: `csp-ticketing-api` has to declare its queues for
  `cine.auth.user-registered.v1` and `cine.auth.user-email-changed.v1` first, otherwise every event is unroutable and fails for good.
- **Only events with a consumer are published.** `AUTH_OUTBOX_RELAY_EVENT_TYPES` lists the event types, `UserRegistered,UserEmailChanged` by default.
  `UserRoleChanged` has no consumer in the MVP: add it to the list when it has one, with no code change. The other types are not read, never tracked and never
  marked failed. A type added later is published from the discovery position of the relay on; rows the position already passed are not looked at again.
  A name that is not an event of the auth `events.md` stops the startup.
- Routing keys come from the event type: `cine.auth.user-registered.v1`, `cine.auth.user-email-changed.v1` and `cine.auth.user-role-changed.v1`.
- The auth envelope has `source` `auth-service` and carries no `aggregateType` or `metadata`, so the relay does not require them and uses the id of the run as the
  correlation id of the message.
- The relay does not guarantee the publication order of the events of the same batch (at-least-once delivery, not ordering): consumers must not depend on it.
- One relay instance per environment: the lock of the auth relay is separate from the booking one, and an instance that does not get it skips the run.
  It needs the migration of `db/` like the booking relay, and the same `RELAY_*` variables.

### Concessions outbox relay

Same relay, reading `concessions.outbox_event` (role `concessions_outbox_reader`, granted by `csp-concessions-db` to `worker_app`), with its own publication
state key (`concessions`), its own PostgreSQL advisory lock and its own broker connection. Delivery is at-least-once, with the same envelope check, backoff and
permanent failures; an unroutable message is a permanent failure.

- **Off by default** (`CONCESSIONS_OUTBOX_RELAY_ENABLED=false`). **Do not turn it on before the consumers exist**: `csp-ticketing-api` has to declare its queue for
  `cine.concessions.order-confirmed.v1` first, otherwise every event is unroutable and fails for good.
- **Only events with a consumer are published.** `CONCESSIONS_OUTBOX_RELAY_EVENT_TYPES` lists the event types, `ConcessionOrderConfirmed` by default (the one Ticketing
  consumes). `ConcessionOrderCreated`, `ConcessionStockReserved` and `ConcessionOrderCancelled` have no consumer yet: add them when they have one, with no code change.
  A name that is not an event of the concessions `events.md` stops the startup.
- **Routing keys** are `cine.concessions.order-created.v1`, `stock-reserved`, `order-confirmed` and `order-cancelled`: the key drops the word "Concession" of the event type.
- The envelope has `source` `concessions-service`, `aggregateType` `ConcessionOrder` and the correlation id of the request in `metadata`, so the relay requires them.
- The relay does not guarantee the publication order of the events of the same batch: consumers must not depend on it. One relay instance per environment, enforced by the lock.

### Ticketing outbox relay

Same relay, reading `ticketing_fulfillment.outbox_event` (role `ticketing_outbox_reader`, created by `csp-ticketing-db` V007), with its own publication state
key (`ticketing_fulfillment`), its own PostgreSQL advisory lock and its own broker connection. Delivery is at-least-once, with the same envelope check, backoff
and permanent failures; an unroutable message is a permanent failure.

- **Off by default** (`TICKETING_OUTBOX_RELAY_ENABLED=false`). **Do not turn it on before the queue exists**: `csp-ticketing-api` declares a queue bound to
  `cine.ticketing_fulfillment.ticket-issued.v1`, otherwise every event is unroutable and fails for good.
- **Database access:** the role `ticketing_outbox_reader` has to be granted to the login `worker_app` by the infrastructure of each environment; the migration only creates it.
- **Events:** `TicketIssued` only; any other name in `TICKETING_OUTBOX_RELAY_EVENT_TYPES` stops the startup. Routing key `cine.ticketing_fulfillment.ticket-issued.v1`.
- The envelope has `source` `ticketing-fulfillment-service`, `aggregateType` `Ticket` and the correlation id of the call that registered the PDF in `metadata`, so the relay requires them.
- The relay does not guarantee the publication order. One relay instance per environment, enforced by the lock.

### Reservation events of Concessions

Off by default (`CONCESSIONS_EVENTS_ENABLED=false`). A run takes up to `CONCESSIONS_EVENTS_BATCH_SIZE` messages (100) in at most
`CONCESSIONS_EVENTS_RUN_TIMEOUT` seconds (20), one at a time, and acknowledges each only after the concessions service accepted it. The
service answers `204` also when the reservation has no order or the order is already in that state, so a repeated message, or a crash between
the call and the acknowledgement, is harmless (no inbox is needed for this job).

- **Queues.** At start the job declares, with the broker login of Concessions (`CONCESSIONS_AMQP_URL`, user `concessions` of ADR-036, not the
  publishing login of the relays), `cine.concessions.booking-confirmed` and `cine.concessions.reservation-expired`, bound to `cine.events` by
  `cine.booking.booking-confirmed.v1` and `cine.booking.reservation-expired.v1`, each with a dead-letter queue (`.dlq`) bound to `cine.dlx`. The exchanges
  are only checked: `csp-infra-rabbitmq` creates them.
- **Failures.** A message the service refuses for good (`400`, `404`, or a `409` because the order is already in the opposite final state) or that is not
  a reservation event goes to the dead-letter queue and the batch continues. When the service cannot be reached, is busy or refuses the token (`401`,
  `403`: the secret of the worker is wrong, not the message), the message goes back to the queue and the run stops. The calls follow the same bounded
  retry with backoff and full jitter as the other clients; a `4xx` is never retried.
- It needs `SERVICE_TOKEN` or `SERVICE_TOKEN_FILE` like the other jobs that call a service, and the token's subject must be in `SERVICE_SUBJECTS` of the concessions service.
- **Do not turn it on before the broker exists.** An order tied to a reservation is settled only by this job (or by the client cancelling it).

### Light jobs and heavy jobs in one worker

The worker is the home of every background process (Norma 4.3), and its jobs are not all alike:

| | Light jobs | Heavy jobs |
|---|---|---|
| Examples | `expire-holds`, `purge-auth`, `outbox-relay` | rendering a PDF or QR ticket, reports, exports |
| Trigger | A timer: a short run every few seconds or minutes | An event taken from a queue, one unit of work at a time |
| Cost | Milliseconds, bounded by a batch (100 events, 20 s; 500 rows, 60 s) | Seconds of CPU and memory per unit |
| What breaks if it is late | An outbox that grows and seats that stay held | Only the one file that is waiting |

They live together under three rules that the code already follows:

1. **One job, one flag, one thread.** Each job is a `ScheduledJob` with its own interval and its own thread, and it exists
   only when its flag is on (`EXPIRE_HOLDS_ENABLED`, `PURGE_AUTH_ENABLED`, `OUTBOX_RELAY_ENABLED`, `AUTH_OUTBOX_RELAY_ENABLED`, `CONCESSIONS_OUTBOX_RELAY_ENABLED`, `TICKETING_OUTBOX_RELAY_ENABLED`, `CONCESSIONS_EVENTS_ENABLED`, `EXPIRE_CONCESSIONS_ORDERS_ENABLED`, and the one of the next job). A slow job
   never delays the others, and an instance without `expire-holds` or `purge-auth` needs no `SERVICE_TOKEN`.
2. **Same pattern for every job.** Ports in the core, adapters outside, a bound per run (Norma 5.7), retries with backoff
   and an idempotent effect, so a duplicate delivery is harmless. A new job reuses this pattern; it does not invent a new one.
3. **A job writes only its own state.** The worker never writes the schema of another domain (Annex J). It keeps its
   state in schema `worker` and asks the owner service through an internal operation, as `expire-holds` does.

The same image then runs as the instances the platform needs. Light jobs stay together because they are cheap and the
relay must never wait; a heavy job gets its own instance, so a rendering that eats memory cannot stall the outbox:

```yaml
worker-light:            # relay and expiration: small, always on
  image: csp-worker
  environment: { EXPIRE_HOLDS_ENABLED: "true", PURGE_AUTH_ENABLED: "true", OUTBOX_RELAY_ENABLED: "true" }
worker-heavy:            # the heavy jobs: sized for rendering, scaled on its own
  image: csp-worker
  environment: { EXPIRE_HOLDS_ENABLED: "false", PURGE_AUTH_ENABLED: "false", OUTBOX_RELAY_ENABLED: "false" }  # plus the flag of the heavy job
```

Today only the three light jobs exist. The ticket PDF belongs to Ticketing & Fulfillment (`BookingConfirmed` starts it),
and it is not implemented here: if the team decides it runs in this worker, it is one more `ScheduledJob` or a queue
consumer built on the pattern above, with its own flag, and no change to the existing jobs. The outbox relay is the piece
that carries `BookingConfirmed` from Booking to the broker, so that flow has a way in.

## Configuration

| Variable | Description | Default |
|---|---|---|
| `SERVICE_TOKEN` | Service token issued by the identity service; fixed for the life of the process | one of the two is required while `expire-holds` or `purge-auth` is on |
| `SERVICE_TOKEN_FILE` | File holding the token, read again at every sweep so the secret can be rotated without a restart | one of the two is required |
| `BOOKING_API_URL` | Booking service internal URL | `http://booking-api:8083/api/v1/booking` |
| `EXPIRE_HOLDS_ENABLED` | Turns the expiration sweep on; off, the instance needs no service token | `true` |
| `EXPIRE_EVERY` | Scheduler interval in seconds of `expire-holds` | `60` |
| `AUTH_API_URL` | Identity service internal URL | `http://auth-api:8081/api/v1/auth` |
| `PURGE_AUTH_ENABLED` | Turns the purge of the identity service on; off, it needs no auth configuration | `false` |
| `PURGE_EVERY` | Scheduler interval in seconds of `purge-auth` | `3600` |
| `PURGE_BATCH_SIZE` | Rows asked per call of `purge-auth`, from 1 to 500; the worker refuses to start otherwise | `500` |
| `PURGE_RUN_TIMEOUT` | Seconds a run of `purge-auth` may keep calling before it stops | `60` |
| `OUTBOX_RELAY_ENABLED` | Turns the outbox relay on; it then needs the database and broker variables below | `false` |
| `OUTBOX_RELAY_EVERY` | Scheduler interval in seconds of `outbox-relay` | `5` |
| `AUTH_OUTBOX_RELAY_ENABLED` | Turns the auth outbox relay on; it needs the same database and broker variables, and the consumers of Ticketing declared first | `false` |
| `AUTH_OUTBOX_RELAY_EVERY` | Scheduler interval in seconds of `auth-outbox-relay` | `5` |
| `AUTH_OUTBOX_RELAY_EVENT_TYPES` | Comma-separated auth event types to publish: `UserRegistered`, `UserEmailChanged`, `UserRoleChanged` | `UserRegistered,UserEmailChanged` |
| `EXPIRE_CONCESSIONS_ORDERS_ENABLED` | Turns the sweep of the orders of Concessions on; it needs `CONCESSIONS_API_URL` and the service token | `false` |
| `EXPIRE_CONCESSIONS_ORDERS_EVERY` | Scheduler interval in seconds of `expire-concessions-orders` | `60` |
| `EXPIRE_CONCESSIONS_ORDERS_BATCH_SIZE` | Orders asked per call, from 1 to 500; the worker refuses to start otherwise | `100` |
| `EXPIRE_CONCESSIONS_ORDERS_RUN_TIMEOUT` | Seconds a run may keep calling before it stops | `60` |
| `CONCESSIONS_OUTBOX_RELAY_ENABLED` | Turns the concessions outbox relay on; it needs the same database and broker variables, and the consumer of Ticketing declared first | `false` |
| `CONCESSIONS_OUTBOX_RELAY_EVERY` | Scheduler interval in seconds of `concessions-outbox-relay` | `5` |
| `CONCESSIONS_OUTBOX_RELAY_EVENT_TYPES` | Comma-separated concessions event types to publish: `ConcessionOrderCreated`, `ConcessionStockReserved`, `ConcessionOrderConfirmed`, `ConcessionOrderCancelled` | `ConcessionOrderConfirmed` |
| `TICKETING_OUTBOX_RELAY_ENABLED` | Turns the ticketing outbox relay on; it needs the same database and broker variables, and the queue bound to `cine.ticketing_fulfillment.ticket-issued.v1` declared first | `false` |
| `TICKETING_OUTBOX_RELAY_EVERY` | Scheduler interval in seconds of `ticketing-outbox-relay` | `5` |
| `TICKETING_OUTBOX_RELAY_EVENT_TYPES` | Comma-separated ticketing event types to publish: `TicketIssued` | `TicketIssued` |
| `CONCESSIONS_EVENTS_ENABLED` | Turns the settlement of the orders of the reservations on; it needs `CONCESSIONS_AMQP_URL` and the service token | `false` |
| `CONCESSIONS_EVENTS_EVERY` | Scheduler interval in seconds of `concessions-reservation-events` | `5` |
| `CONCESSIONS_EVENTS_BATCH_SIZE` | Messages a run may take, at least 1 | `100` |
| `CONCESSIONS_EVENTS_RUN_TIMEOUT` | Seconds a run may keep taking messages before it stops | `20` |
| `CONCESSIONS_API_URL` | Concessions service internal URL | `http://concessions-api:8085/api/v1/concessions` |
| `CONCESSIONS_AMQP_URL` | RabbitMQ URI with the login of Concessions (`concessions`, ADR-036) | required when the job is on |
| `CONCESSIONS_AMQP_EXCHANGE` / `CONCESSIONS_AMQP_DEAD_LETTER_EXCHANGE` | Events exchange and dead-letter exchange, checked at start | `cine.events` / `cine.dlx` |
| `RELAY_DATABASE_URL` / `RELAY_DATABASE_USER` / `RELAY_DATABASE_PASSWORD` | PostgreSQL login of the relays (`worker_app`) | user `worker_app` |
| `RELAY_AMQP_URL` | RabbitMQ URI with the login `worker_relay` (ADR-036). The default virtual host is written `/%2F` (`amqp://worker_relay:<password>@rabbitmq:5672/%2F`): a URL ending in a bare `/` means an empty virtual host and stops the startup | required when a relay is on |
| `RELAY_AMQP_EXCHANGE` | Topic exchange the events are published to | `cine.events` |

## Build and run

```bash
mvn clean package
java -jar worker-app/target/worker-app-0.1.0.jar
```

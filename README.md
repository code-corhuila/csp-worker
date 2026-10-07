# csp-worker

Scheduled background jobs for the CineSync Platform (Norma 4.3). The worker runs jobs that no one
asks for: expiration sweeps, notifications, reindexation. It exposes no business interface, only
a health endpoint (Norma 5.7.1).

## Jobs

| Job | Schedule | Description |
|---|---|---|
| `expire-holds` | Every 60s | Calls `POST /internal/maintenance/expire-holds` on the booking service to expire HELD reservations past their hold time (HU-BOOKING-002) |
| `outbox-relay` | Every 5s, when enabled | Publishes the events the booking API wrote to `booking.outbox_event` to RabbitMQ, with at-least-once delivery ([ADR-014](https://github.com/code-corhuila/csp-docs/blob/main/05-architecture/decisions/records/ADR-014-booking-outbox-relay-read-only.md)) |

### Outbox relay

Off by default. It reads the booking outbox with `SELECT` only, through the `worker_app` login, and keeps what it
has published in schema `worker` (`outbox_relay_state`, `outbox_relay_cursor`); it never writes the booking schema.
A run publishes at most 100 events in 20 seconds. A failed publication is retried with exponential backoff (1 second
up to 5 minutes, 20 attempts); an invalid envelope or a message no queue is bound to fails for good and is replayed by
an operator setting `status = 'PENDING'` and `attempt_count = 0` in `worker.outbox_relay_state`.

To enable it: create the schema with the migration of `db/` (`docker compose --profile tooling run --rm
worker-db-migrate`, as the instance administrator), point `RELAY_AMQP_URL` at the broker and set
`OUTBOX_RELAY_ENABLED=true`. Consumers deduplicate by `eventId`, the `messageId` of every message.

## Configuration

| Variable | Description | Default |
|---|---|---|
| `SERVICE_TOKEN` | Service token issued by the identity service; fixed for the life of the process | one of the two is required |
| `SERVICE_TOKEN_FILE` | File holding the token, read again at every sweep so the secret can be rotated without a restart | one of the two is required |
| `BOOKING_API_URL` | Booking service internal URL | `http://booking-api:8083/api/v1/booking` |
| `EXPIRE_EVERY` | Scheduler interval in seconds of `expire-holds` | `60` |
| `OUTBOX_RELAY_ENABLED` | Turns the outbox relay on; it then needs the database and broker variables below | `false` |
| `OUTBOX_RELAY_EVERY` | Scheduler interval in seconds of `outbox-relay` | `5` |
| `RELAY_DATABASE_URL` / `RELAY_DATABASE_USER` / `RELAY_DATABASE_PASSWORD` | PostgreSQL login of the relay (`worker_app`) | user `worker_app` |
| `RELAY_AMQP_URL` | RabbitMQ URI | required when the relay is on |
| `RELAY_AMQP_EXCHANGE` | Topic exchange the events are published to | `cine.events` |

## Build and run

```bash
mvn clean package
java -jar worker-app/target/worker-app-0.1.0.jar
```

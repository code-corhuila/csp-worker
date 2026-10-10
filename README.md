# csp-worker

Scheduled background jobs for the CineSync Platform (Norma 4.3). The worker runs jobs that no one
asks for: expiration sweeps, notifications, reindexation. It exposes no business interface
(Norma 5.7.1 admits at most a health endpoint, which is not implemented yet: code-corhuila/csp-worker#18).

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

Run the relay as a **single instance per environment**: turn the flag on in one instance only. Its scheduler never overlaps two
runs of the same instance, but two instances would not coordinate. They could publish the same event twice, which the consumers
absorb, and both could write the same attempt count from the same read, so an event would get a few more attempts than the
count says; a stale read can only be lower than the real count, so it never exhausts an event early. A lock would be held
across the call to the broker, which is worse, so no coordination is built until a decision asks for more than one relay.

Before enabling it in a shared environment, the consumers must have declared their queues: a message that no queue is bound
to is a permanent failure and waits for an operator. The name of the exchange (`cine.events`) is an assumption of this work,
not a documented one, until the consumers confirm it.

### Light jobs and heavy jobs in one worker

The worker is the home of every background process (Norma 4.3), and its jobs are not all alike:

| | Light jobs | Heavy jobs |
|---|---|---|
| Examples | `expire-holds`, `outbox-relay` | rendering a PDF or QR ticket, reports, exports |
| Trigger | A timer: a short run every few seconds or minutes | An event taken from a queue, one unit of work at a time |
| Cost | Milliseconds, bounded by a batch (100 events, 20 s) | Seconds of CPU and memory per unit |
| What breaks if it is late | An outbox that grows and seats that stay held | Only the one file that is waiting |

They live together under three rules that the code already follows:

1. **One job, one flag, one thread.** Each job is a `ScheduledJob` with its own interval and its own thread, and it exists
   only when its flag is on (`EXPIRE_HOLDS_ENABLED`, `OUTBOX_RELAY_ENABLED`, and the one of the next job). A slow job
   never delays the others, and an instance without the expiration sweep needs no `SERVICE_TOKEN`.
2. **Same pattern for every job.** Ports in the core, adapters outside, a bound per run (Norma 5.7), retries with backoff
   and an idempotent effect, so a duplicate delivery is harmless. A new job reuses this pattern; it does not invent a new one.
3. **A job writes only its own state.** The worker never writes the schema of another domain (Annex J). It keeps its
   state in schema `worker` and asks the owner service through an internal operation, as `expire-holds` does.

The same image then runs as the instances the platform needs. Light jobs stay together because they are cheap and the
relay must never wait; a heavy job gets its own instance, so a rendering that eats memory cannot stall the outbox:

```yaml
worker-light:            # relay and expiration: small, always on
  image: csp-worker
  environment: { EXPIRE_HOLDS_ENABLED: "true", OUTBOX_RELAY_ENABLED: "true" }
worker-heavy:            # the heavy jobs: sized for rendering, scaled on its own
  image: csp-worker
  environment: { EXPIRE_HOLDS_ENABLED: "false", OUTBOX_RELAY_ENABLED: "false" }  # plus the flag of the heavy job
```

Today only the two light jobs exist. The ticket PDF belongs to Ticketing & Fulfillment (`BookingConfirmed` starts it),
and it is not implemented here: if the team decides it runs in this worker, it is one more `ScheduledJob` or a queue
consumer built on the pattern above, with its own flag, and no change to the existing jobs. The outbox relay is the piece
that carries `BookingConfirmed` from Booking to the broker, so that flow has a way in.

## Configuration

| Variable | Description | Default |
|---|---|---|
| `SERVICE_TOKEN` | Service token issued by the identity service; fixed for the life of the process | one of the two is required |
| `SERVICE_TOKEN_FILE` | File holding the token, read again at every sweep so the secret can be rotated without a restart | one of the two is required |
| `BOOKING_API_URL` | Booking service internal URL | `http://booking-api:8083/api/v1/booking` |
| `EXPIRE_HOLDS_ENABLED` | Turns the expiration sweep on; off, the instance needs no service token | `true` |
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

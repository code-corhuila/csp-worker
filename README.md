# csp-worker

Scheduled background jobs for the CineSync Platform (Norma 4.3). The worker runs jobs that no one
asks for: expiration sweeps, notifications, reindexation. It exposes no business interface, only
a health endpoint (Norma 5.7.1).

## Jobs

| Job | Schedule | Description |
|---|---|---|
| `expire-holds` | Every 60s | Calls `POST /internal/maintenance/expire-holds` on the booking service to expire HELD reservations past their hold time (HU-BOOKING-002) |

## Configuration

| Variable | Description | Default |
|---|---|---|
| `SERVICE_TOKEN` | Service token issued by the identity service | (required) |
| `BOOKING_API_URL` | Booking service internal URL | `http://booking-api:8083/api/v1/booking` |
| `EXPIRE_EVERY` | Scheduler interval in seconds | `60` |

## Build and run

```bash
mvn clean package
java -jar worker-app/target/worker-app-0.1.0.jar
```

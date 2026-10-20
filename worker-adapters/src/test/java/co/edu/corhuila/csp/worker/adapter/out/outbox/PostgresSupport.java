package co.edu.corhuila.csp.worker.adapter.out.outbox;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Statement;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * A real PostgreSQL for the adapter tests, the same way the booking API tests get one: the tests
 * run when {@code TEST_DATABASE_URL} is set and are skipped otherwise. The worker schema is the
 * migration of {@code db/} applied as it is; the outbox tables are the ones of {@code csp-booking-db}
 * and {@code csp-auth-db} reduced to the columns the relay reads, because the schemas of those domains live there.
 */
final class PostgresSupport {

    private static final Path MIGRATION = Path.of("..", "db", "V001__create_outbox_relay_state.sql");

    private PostgresSupport() {
    }

    static JdbcTemplate freshDatabase() {
        JdbcTemplate jdbc = new JdbcTemplate(new DriverManagerDataSource(System.getenv("TEST_DATABASE_URL")));
        jdbc.execute("""
                DO $$ BEGIN
                  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'worker_app') THEN
                    CREATE ROLE worker_app NOLOGIN;
                  END IF;
                END $$
                """);
        jdbc.execute("DROP SCHEMA IF EXISTS worker CASCADE");
        jdbc.execute("DROP SCHEMA IF EXISTS booking CASCADE");
        jdbc.execute("DROP SCHEMA IF EXISTS auth CASCADE");
        jdbc.execute("CREATE SCHEMA auth");
        jdbc.execute("""
                CREATE TABLE auth.outbox_event (
                    id             uuid        NOT NULL,
                    aggregate_type text        NOT NULL,
                    aggregate_id   uuid        NOT NULL,
                    event_type     text        NOT NULL,
                    payload        jsonb       NOT NULL,
                    created_at     timestamptz NOT NULL DEFAULT NOW(),
                    CONSTRAINT pk_auth_outbox_event PRIMARY KEY (id)
                )
                """);
        jdbc.execute("CREATE SCHEMA booking");
        jdbc.execute("""
                CREATE TABLE booking.outbox_event (
                    id             UUID         NOT NULL DEFAULT gen_random_uuid(),
                    aggregate_type VARCHAR(100) NOT NULL,
                    aggregate_id   UUID         NOT NULL,
                    event_type     VARCHAR(100) NOT NULL,
                    payload        JSONB        NOT NULL,
                    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
                    CONSTRAINT pk_outbox_event PRIMARY KEY (id)
                )
                """);
        jdbc.execute((ConnectionCallback<Void>) connection -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute(Files.readString(MIGRATION));
            } catch (java.io.IOException exception) {
                throw new IllegalStateException("cannot read " + MIGRATION.toAbsolutePath(), exception);
            }
            return null;
        });
        return jdbc;
    }

    /** A row of the auth outbox the way {@code csp-auth-api} writes it: no aggregate type or metadata inside the envelope. */
    static java.util.UUID insertAuthEvent(JdbcTemplate jdbc, String eventType, String createdAt) {
        java.util.UUID id = java.util.UUID.randomUUID();
        java.util.UUID user = java.util.UUID.randomUUID();
        String envelope = "{\"eventId\":\"" + id + "\",\"eventType\":\"" + eventType + "\",\"occurredAt\":\""
                + createdAt + "\",\"version\":1,\"source\":\"auth-service\",\"aggregateId\":\"" + user
                + "\",\"payload\":{\"userId\":\"" + user + "\"}}";
        jdbc.update("INSERT INTO auth.outbox_event (id, aggregate_type, aggregate_id, event_type, payload, "
                + "created_at) VALUES (?, 'User', ?, ?, ?::jsonb, ?::timestamptz)", id, user, eventType, envelope, createdAt);
        return id;
    }

    static void insertEvent(JdbcTemplate jdbc, java.util.UUID id, String eventType, String createdAt,
            String correlationId) {
        String envelope = "{\"eventId\":\"" + id + "\",\"eventType\":\"" + eventType + "\",\"metadata\":{"
                + "\"correlationId\":\"" + correlationId + "\"}}";
        jdbc.update("INSERT INTO booking.outbox_event (id, aggregate_type, aggregate_id, event_type, payload, "
                + "created_at) VALUES (?, 'Reservation', gen_random_uuid(), ?, ?::jsonb, ?::timestamptz)",
                id, eventType, envelope, createdAt);
    }
}

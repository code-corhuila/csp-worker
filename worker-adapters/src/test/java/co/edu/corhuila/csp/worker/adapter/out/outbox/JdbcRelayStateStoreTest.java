package co.edu.corhuila.csp.worker.adapter.out.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.application.port.out.OutboxPosition;
import co.edu.corhuila.csp.worker.application.port.out.PendingEvent;
import co.edu.corhuila.csp.worker.application.port.out.RelayStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;

/** The publication state of the relay in schema {@code worker} against a real PostgreSQL. */
@EnabledIfEnvironmentVariable(named = "TEST_DATABASE_URL", matches = ".+")
class JdbcRelayStateStoreTest {

    private static final Instant NOW = Instant.parse("2026-10-07T12:00:00Z");
    private static final UUID A = UUID.fromString("00000000-0000-4000-8000-00000000000a");
    private static final UUID B = UUID.fromString("00000000-0000-4000-8000-00000000000b");

    private JdbcTemplate jdbc;
    private JdbcRelayStateStore store;

    @BeforeEach
    void setUp() {
        jdbc = PostgresSupport.freshDatabase();
        store = new JdbcRelayStateStore(jdbc);
    }

    private Map<String, Object> row(UUID id) {
        return jdbc.queryForMap("SELECT * FROM worker.outbox_relay_state WHERE event_id = ?", id);
    }

    @Test
    void thereIsNoCursorBeforeTheFirstRun() {
        assertTrue(store.cursor().isEmpty());
    }

    @Test
    void theCursorIsSavedAndOverwritten() {
        store.saveCursor(new OutboxPosition(NOW, A));
        store.saveCursor(new OutboxPosition(NOW.plusSeconds(5), B));

        assertEquals(new OutboxPosition(NOW.plusSeconds(5), B), store.cursor().orElseThrow());
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM worker.outbox_relay_cursor", Integer.class));
    }

    @Test
    void trackedEventsStartPendingAndDueNow() {
        store.track(List.of(A, B), NOW);

        assertEquals("PENDING", row(A).get("status"));
        assertEquals(0, row(A).get("attempt_count"));
        assertEquals("booking", row(A).get("source"));
        assertEquals(2, store.duePending(NOW, 10).size());
    }

    @Test
    void trackingAnEventAgainKeepsItsState() {
        store.track(List.of(A), NOW);
        store.markRetry(A, 3, NOW.plusSeconds(60), "broker down");

        store.track(List.of(A), NOW.plusSeconds(1));

        assertEquals(3, row(A).get("attempt_count"));
        assertEquals(List.of(), store.duePending(NOW, 10));
    }

    @Test
    void anEventIsDueOnlyWhenItsNextAttemptHasArrived() {
        store.track(List.of(A), NOW);
        store.markRetry(A, 1, NOW.plusSeconds(10), "broker down");

        assertTrue(store.duePending(NOW.plusSeconds(9), 10).isEmpty());
        assertEquals(List.of(new PendingEvent(A, 1)), store.duePending(NOW.plusSeconds(10), 10));
    }

    @Test
    void theLimitBoundsTheDueEvents() {
        store.track(List.of(A, B), NOW);

        assertEquals(1, store.duePending(NOW, 1).size());
    }

    @Test
    void aPublishedEventRecordsWhenAndCountsTheAttempt() {
        store.track(List.of(A), NOW);
        store.markRetry(A, 2, NOW, "broker down");

        store.markPublished(A, NOW.plusSeconds(3));

        assertEquals("PUBLISHED", row(A).get("status"));
        assertEquals(3, row(A).get("attempt_count"));
        assertEquals(NOW.plusSeconds(3), ((java.sql.Timestamp) row(A).get("published_at")).toInstant());
        assertNull(row(A).get("last_error"));
        assertTrue(store.duePending(NOW.plusSeconds(60), 10).isEmpty());
    }

    @Test
    void aRetryRecordsTheAttemptsTheErrorAndTheNextAttempt() {
        store.track(List.of(A), NOW);

        store.markRetry(A, 4, NOW.plusSeconds(8), "PublishException: broker connection lost");

        assertEquals("PENDING", row(A).get("status"));
        assertEquals(4, row(A).get("attempt_count"));
        assertEquals("PublishException: broker connection lost", row(A).get("last_error"));
        assertEquals(NOW.plusSeconds(8), ((java.sql.Timestamp) row(A).get("next_attempt_at")).toInstant());
    }

    @Test
    void aFailureForGoodLeavesTheDuePendingList() {
        store.track(List.of(A, B), NOW);

        store.markFailed(A, RelayStatus.FAILED_PERMANENT, 0, "payload is missing");
        store.markFailed(B, RelayStatus.FAILED_EXHAUSTED, 20, "broker down");

        assertEquals("FAILED_PERMANENT", row(A).get("status"));
        assertEquals("FAILED_EXHAUSTED", row(B).get("status"));
        assertEquals(20, row(B).get("attempt_count"));
        assertTrue(store.duePending(NOW.plusSeconds(3600), 10).isEmpty());
    }
}

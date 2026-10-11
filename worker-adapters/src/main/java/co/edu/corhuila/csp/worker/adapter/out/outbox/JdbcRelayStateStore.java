package co.edu.corhuila.csp.worker.adapter.out.outbox;

import co.edu.corhuila.csp.worker.application.port.out.OutboxPosition;
import co.edu.corhuila.csp.worker.application.port.out.PendingEvent;
import co.edu.corhuila.csp.worker.application.port.out.RelayStateStore;
import co.edu.corhuila.csp.worker.application.port.out.RelayStatus;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The publication state of the booking relay in schema {@code worker} (ADR-014): the only tables
 * the worker writes. It never writes the booking schema.
 */
public class JdbcRelayStateStore implements RelayStateStore {

    static final String SOURCE = "booking";

    private static final String CURSOR = """
            SELECT last_created_at, last_event_id FROM worker.outbox_relay_cursor WHERE source = ?
            """;

    private static final String SAVE_CURSOR = """
            INSERT INTO worker.outbox_relay_cursor (source, last_created_at, last_event_id) VALUES (?, ?, ?)
            ON CONFLICT (source) DO UPDATE
               SET last_created_at = EXCLUDED.last_created_at, last_event_id = EXCLUDED.last_event_id
            """;

    private static final String TRACK = """
            INSERT INTO worker.outbox_relay_state (event_id, source, status, attempt_count, next_attempt_at)
            VALUES (?, ?, 'PENDING', 0, ?)
            ON CONFLICT (event_id) DO NOTHING
            """;

    private static final String DUE = """
            SELECT event_id, attempt_count FROM worker.outbox_relay_state
            WHERE source = ? AND status = 'PENDING' AND next_attempt_at <= ?
            ORDER BY next_attempt_at, event_id LIMIT ?
            """;

    private static final String PUBLISHED = """
            UPDATE worker.outbox_relay_state
               SET status = 'PUBLISHED', published_at = ?, attempt_count = attempt_count + 1, last_error = NULL
             WHERE event_id = ?
            """;

    private static final String RETRY = """
            UPDATE worker.outbox_relay_state
               SET status = 'PENDING', attempt_count = ?, next_attempt_at = ?, last_error = ?
             WHERE event_id = ?
            """;

    private static final String FAILED = """
            UPDATE worker.outbox_relay_state SET status = ?, attempt_count = ?, last_error = ? WHERE event_id = ?
            """;

    private final JdbcTemplate jdbc;

    public JdbcRelayStateStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<OutboxPosition> cursor() {
        return jdbc.query(CURSOR, (row, index) -> new OutboxPosition(row.getTimestamp("last_created_at").toInstant(),
                row.getObject("last_event_id", UUID.class)), SOURCE).stream().findFirst();
    }

    @Override
    public void saveCursor(OutboxPosition position) {
        jdbc.update(SAVE_CURSOR, SOURCE, Timestamp.from(position.createdAt()), position.id());
    }

    @Override
    public void track(Collection<UUID> eventIds, Instant now) {
        jdbc.batchUpdate(TRACK, eventIds, eventIds.size(), (statement, id) -> {
            statement.setObject(1, id);
            statement.setString(2, SOURCE);
            statement.setTimestamp(3, Timestamp.from(now));
        });
    }

    @Override
    public List<PendingEvent> duePending(Instant now, int limit) {
        return jdbc.query(DUE, (row, index) -> new PendingEvent(row.getObject("event_id", UUID.class),
                row.getInt("attempt_count")), SOURCE, Timestamp.from(now), limit);
    }

    @Override
    public void markPublished(UUID eventId, Instant now) {
        jdbc.update(PUBLISHED, Timestamp.from(now), eventId);
    }

    @Override
    public void markRetry(UUID eventId, int attemptCount, Instant nextAttemptAt, String lastError) {
        jdbc.update(RETRY, attemptCount, Timestamp.from(nextAttemptAt), lastError, eventId);
    }

    @Override
    public void markFailed(UUID eventId, RelayStatus status, int attemptCount, String lastError) {
        jdbc.update(FAILED, status.name(), attemptCount, lastError, eventId);
    }
}

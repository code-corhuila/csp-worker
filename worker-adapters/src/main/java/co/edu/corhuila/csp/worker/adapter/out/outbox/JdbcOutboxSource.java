package co.edu.corhuila.csp.worker.adapter.out.outbox;

import co.edu.corhuila.csp.worker.application.port.out.OutboxEvent;
import co.edu.corhuila.csp.worker.application.port.out.OutboxPosition;
import co.edu.corhuila.csp.worker.application.port.out.OutboxSource;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 * Reads {@code booking.outbox_event} with {@code SELECT} and nothing else (ADR-014). The connection
 * is the one of the {@code worker_app} login, whose only privilege on the booking schema is the
 * {@code SELECT} of the role {@code booking_outbox_reader}; no statement here writes it.
 */
public class JdbcOutboxSource implements OutboxSource {

    private static final String COLUMNS = """
            SELECT id, event_type, aggregate_id, payload::text AS envelope,
                   payload -> 'metadata' ->> 'correlationId' AS correlation_id, created_at
            FROM booking.outbox_event
            """;

    private static final String DISCOVER = COLUMNS
            + " WHERE (created_at, id) > (?, ?) ORDER BY created_at, id LIMIT ?";

    private static final String BY_IDS = COLUMNS + " WHERE id = ANY (?) ORDER BY created_at, id";

    private static final RowMapper<OutboxEvent> ROW = JdbcOutboxSource::toEvent;

    private final JdbcTemplate jdbc;

    public JdbcOutboxSource(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public List<OutboxEvent> discover(OutboxPosition after, int limit) {
        return jdbc.query(DISCOVER, ROW, Timestamp.from(after.createdAt()), after.id(), limit);
    }

    @Override
    public List<OutboxEvent> findByIds(Collection<UUID> ids) {
        if (ids.isEmpty()) {
            return List.of();
        }
        return jdbc.query(connection -> {
            Array array = connection.createArrayOf("uuid", ids.toArray());
            var statement = connection.prepareStatement(BY_IDS);
            statement.setArray(1, array);
            return statement;
        }, ROW);
    }

    private static OutboxEvent toEvent(ResultSet row, int index) throws SQLException {
        Instant createdAt = row.getTimestamp("created_at").toInstant();
        return new OutboxEvent(row.getObject("id", UUID.class), row.getString("event_type"),
                row.getObject("aggregate_id", UUID.class), row.getString("envelope"),
                row.getString("correlation_id"), createdAt);
    }
}

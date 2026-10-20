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
 * Reads {@code <schema>.outbox_event} of one domain with {@code SELECT} and nothing else (ADR-014,
 * ADR-019). The connection is the one of the {@code worker_app} login, whose only privilege on the
 * schema is the {@code SELECT} of the role {@code <domain>_outbox_reader}; no statement here writes it.
 *
 * <p>When {@code eventTypes} is not empty, discovery returns only rows of those types: the others are
 * never tracked, so they are never published nor marked failed. A type added to the set later is
 * published from the discovery position on; the rows the position already passed are not looked at again.
 */
public class JdbcOutboxSource implements OutboxSource {

    private static final RowMapper<OutboxEvent> ROW = JdbcOutboxSource::toEvent;

    private final JdbcTemplate jdbc;
    private final String discoverQuery;
    private final String byIdsQuery;
    private final String[] eventTypes;

    /** @param eventTypes the types to read; empty reads every type of the outbox */
    public JdbcOutboxSource(JdbcTemplate jdbc, RelayDomain domain, Collection<String> eventTypes) {
        this.jdbc = jdbc;
        String columns = """
                SELECT id, event_type, aggregate_id, payload::text AS envelope,
                       payload -> 'metadata' ->> 'correlationId' AS correlation_id, created_at
                FROM """ + " " + domain.schema() + ".outbox_event";
        this.eventTypes = eventTypes.toArray(String[]::new);
        String typeFilter = this.eventTypes.length == 0 ? "" : " AND event_type = ANY (?)";
        this.discoverQuery = columns + " WHERE (created_at, id) > (?, ?)" + typeFilter
                + " ORDER BY created_at, id LIMIT ?";
        this.byIdsQuery = columns + " WHERE id = ANY (?) ORDER BY created_at, id";
    }

    @Override
    public List<OutboxEvent> discover(OutboxPosition after, int limit) {
        return jdbc.query(connection -> {
            var statement = connection.prepareStatement(discoverQuery);
            statement.setTimestamp(1, Timestamp.from(after.createdAt()));
            statement.setObject(2, after.id());
            int next = 3;
            if (eventTypes.length > 0) {
                statement.setArray(next++, connection.createArrayOf("text", eventTypes));
            }
            statement.setInt(next, limit);
            return statement;
        }, ROW);
    }

    @Override
    public List<OutboxEvent> findByIds(Collection<UUID> ids) {
        if (ids.isEmpty()) {
            return List.of();
        }
        return jdbc.query(connection -> {
            Array array = connection.createArrayOf("uuid", ids.toArray());
            var statement = connection.prepareStatement(byIdsQuery);
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

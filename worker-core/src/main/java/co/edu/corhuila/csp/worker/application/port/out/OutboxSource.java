package co.edu.corhuila.csp.worker.application.port.out;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Read-only view of the booking outbox (ADR-014): the implementation holds {@code SELECT} on
 * {@code booking.outbox_event} and nothing else in the booking schema.
 */
public interface OutboxSource {

    /** At most {@code limit} rows after {@code after}, ordered by {@code (created_at, id)}. */
    List<OutboxEvent> discover(OutboxPosition after, int limit);

    /** The rows with those ids that still exist in the outbox. */
    List<OutboxEvent> findByIds(Collection<UUID> ids);
}

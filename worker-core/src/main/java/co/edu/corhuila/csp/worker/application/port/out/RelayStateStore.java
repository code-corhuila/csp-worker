package co.edu.corhuila.csp.worker.application.port.out;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * The publication state of the relay, kept in the schema {@code worker} and never in the booking
 * schema (ADR-014).
 */
public interface RelayStateStore {

    /** The discovery position of the last run, empty before the first one. */
    Optional<OutboxPosition> cursor();

    void saveCursor(OutboxPosition position);

    /** Starts tracking the events as {@code PENDING}; the ones already tracked are left as they are. */
    void track(Collection<UUID> eventIds, Instant now);

    /** {@code PENDING} events whose next attempt is not in the future, at most {@code limit}. */
    List<PendingEvent> duePending(Instant now, int limit);

    void markPublished(UUID eventId, Instant now);

    void markRetry(UUID eventId, int attemptCount, Instant nextAttemptAt, String lastError);

    void markFailed(UUID eventId, RelayStatus status, int attemptCount, String lastError);
}

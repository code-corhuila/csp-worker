package co.edu.corhuila.csp.worker.application.port.out;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * A point in the outbox order {@code (created_at, id)}. The id breaks the tie of rows created in
 * the same instant, and compares like PostgreSQL does, byte by byte, which is the order of its
 * text form.
 */
public record OutboxPosition(Instant createdAt, UUID id) {

    private static final UUID LOWEST_ID = new UUID(0L, 0L);

    /** Before every row. */
    public static final OutboxPosition START = new OutboxPosition(Instant.EPOCH, LOWEST_ID);

    public static OutboxPosition of(OutboxEvent event) {
        return new OutboxPosition(event.createdAt(), event.id());
    }

    /** The first position of the instant that lies {@code amount} before this one. */
    public OutboxPosition minus(Duration amount) {
        return new OutboxPosition(createdAt.minus(amount), LOWEST_ID);
    }

    public boolean isAfter(OutboxPosition other) {
        int byTime = createdAt.compareTo(other.createdAt);
        return byTime != 0 ? byTime > 0 : id.toString().compareTo(other.id.toString()) > 0;
    }
}

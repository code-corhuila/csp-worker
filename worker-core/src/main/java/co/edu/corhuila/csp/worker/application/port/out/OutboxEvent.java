package co.edu.corhuila.csp.worker.application.port.out;

import java.time.Instant;
import java.util.UUID;

/**
 * A row of {@code booking.outbox_event} as the relay reads it. {@code envelope} is the complete
 * event envelope written by the booking API; the relay publishes it as it is and never rewrites
 * it. {@code correlationId} is {@code metadata.correlationId} of that envelope.
 */
public record OutboxEvent(UUID id, String eventType, UUID aggregateId, String envelope, String correlationId,
        Instant createdAt) {
}

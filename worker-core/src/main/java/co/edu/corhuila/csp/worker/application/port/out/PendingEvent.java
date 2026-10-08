package co.edu.corhuila.csp.worker.application.port.out;

import java.util.UUID;

/** An event that is due for a publication attempt, with the attempts made so far. */
public record PendingEvent(UUID eventId, int attemptCount) {
}

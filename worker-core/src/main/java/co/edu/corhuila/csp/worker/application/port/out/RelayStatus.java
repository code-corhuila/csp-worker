package co.edu.corhuila.csp.worker.application.port.out;

/** Publication state of one event (ADR-014). */
public enum RelayStatus {
    PENDING,
    PUBLISHED,
    /** Invalid envelope, unroutable message or an event that left the outbox: never retried. */
    FAILED_PERMANENT,
    /** The retries ran out: waits for a manual replay. */
    FAILED_EXHAUSTED
}

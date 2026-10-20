package co.edu.corhuila.csp.worker.adapter.out.outbox;

/**
 * A domain whose outbox the relay publishes (ADR-014, ADR-019). Each one has its own schema, its own
 * relay state key, its own instance lock and the envelope its API writes, so two relays in one
 * worker never collide.
 *
 * <p>{@code tracedEnvelope} says whether the envelope carries {@code aggregateType} and
 * {@code metadata.correlationId}: the booking one does, the auth one of {@code events.md} does not,
 * and its correlation id is then the one of the run.
 */
public enum RelayDomain {

    /** The booking outbox, ADR-014. The lock key is the ASCII of "cspoutbx". */
    BOOKING("booking", "booking-service", 0x6373706F75746278L, true),

    /** The auth outbox, ADR-019. The lock key is the ASCII of "cspoutba". */
    AUTH("auth", "auth-service", 0x6373706F75746261L, false);

    private final String schema;
    private final String envelopeSource;
    private final long lockKey;
    private final boolean tracedEnvelope;

    RelayDomain(String schema, String envelopeSource, long lockKey, boolean tracedEnvelope) {
        this.schema = schema;
        this.envelopeSource = envelopeSource;
        this.lockKey = lockKey;
        this.tracedEnvelope = tracedEnvelope;
    }

    /** The schema of the outbox and the value of {@code source} in the relay state; also the routing key domain. */
    public String schema() {
        return schema;
    }

    /** The value of {@code source} an envelope of this domain must carry. */
    public String envelopeSource() {
        return envelopeSource;
    }

    public long lockKey() {
        return lockKey;
    }

    public boolean tracedEnvelope() {
        return tracedEnvelope;
    }
}

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
    AUTH("auth", "auth-service", 0x6373706F75746261L, false),

    /**
     * The concessions outbox, ADR-019 and ADR-045. The lock key is the ASCII of "cspoutbc". Its envelope is the
     * traced one of booking.
     */
    CONCESSIONS("concessions", "concessions-service", 0x6373706F75746263L, true),

    /**
     * The ticketing outbox, ADR-057. The lock key is the ASCII of "cspoutbt". Its envelope is the traced one of
     * booking; the schema name is the one of its routing key, {@code cine.ticketing_fulfillment.ticket-issued.v1}.
     */
    TICKETING("ticketing_fulfillment", "ticketing-fulfillment-service", 0x6373706F75746274L, true);

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

    /**
     * The event type as the routing key spells it: the {@code events.md} of Concessions names the keys
     * {@code order-confirmed} and {@code stock-reserved}, without the word of {@code ConcessionOrderConfirmed}.
     */
    public String routingName(String eventType) {
        String prefix = "Concession";
        return this == CONCESSIONS && eventType.startsWith(prefix) ? eventType.substring(prefix.length()) : eventType;
    }

    public long lockKey() {
        return lockKey;
    }

    public boolean tracedEnvelope() {
        return tracedEnvelope;
    }
}

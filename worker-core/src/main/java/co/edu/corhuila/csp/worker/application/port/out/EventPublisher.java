package co.edu.corhuila.csp.worker.application.port.out;

/** The broker side of the relay: one confirmed, persistent publication per call. */
public interface EventPublisher {

    /**
     * Publishes the envelope of the event with {@code messageId} equal to the event id.
     *
     * @throws PublishException when the broker did not accept it, tagged as retryable or permanent
     */
    void publish(OutboxEvent event, String correlationId);
}

package co.edu.corhuila.csp.worker.application.port.out;

/**
 * The Ticketing service as seen by the worker: the internal operation that creates the warnings of the holds that
 * are about to expire (HU-TICKETING-005, notification {@code HOLD_EXPIRY_WARNING}). The worker never touches the
 * ticketing schema.
 */
public interface TicketingHoldExpiryWarningsApi {

    /**
     * Calls {@code POST /internal/notifications/hold-expiry-warnings}: the service creates the warning of up to
     * {@code limit} holds that are close to their expiry and have none yet, and answers how many it created.
     *
     * @param limit how many warnings the service may create in this call
     * @return how many warnings the service created
     * @throws TicketingApiException when the service did not accept it
     */
    int warn(String correlationId, int limit);
}

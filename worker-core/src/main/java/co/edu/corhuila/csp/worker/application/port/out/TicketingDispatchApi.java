package co.edu.corhuila.csp.worker.application.port.out;

/**
 * The Ticketing service as seen by the worker: the internal operation that sends the emails of the tickets already
 * issued (ADR-057). The worker never touches the ticketing schema.
 */
public interface TicketingDispatchApi {

    /**
     * Calls {@code POST /internal/fulfillments/dispatch}: the service sends the emails of up to {@code limit} due
     * deliveries whose ticket has its PDF, keeps the ones whose contact is not known yet for a later attempt, and
     * answers how many it sent, kept for later and gave up.
     *
     * @param limit how many deliveries the service may handle in this call, from 1 to 100
     * @throws TicketingApiException when the service did not accept it
     */
    Result dispatch(String correlationId, int limit);

    /** What one call did. */
    record Result(int sent, int retried, int failed) {

        public int handled() {
            return sent + retried + failed;
        }
    }
}

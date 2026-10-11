package co.edu.corhuila.csp.worker.application.port.out;

/**
 * The booking service as seen by the worker: only the operations the worker needs. The worker
 * never writes the booking schema (ADR-014).
 */
public interface BookingExpireHoldsApi {

    /**
     * Calls {@code POST /internal/maintenance/expire-holds} on the booking service.
     *
     * @param correlationId the correlation id of this sweep run
     * @return how many reservations were expired and how many remain
     */
    ExpireHoldsResult expireHolds(String correlationId);
}

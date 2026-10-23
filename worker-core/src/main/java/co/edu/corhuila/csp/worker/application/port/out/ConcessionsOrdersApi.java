package co.edu.corhuila.csp.worker.application.port.out;

/**
 * The Concessions service as seen by the worker: only the internal operations that settle the order of a
 * reservation (ADR-045). Both answer without error when the reservation has no order or the order is already in
 * that state, so a repeated message is harmless. The worker never touches the concessions schema.
 */
public interface ConcessionsOrdersApi {

    /**
     * Calls {@code POST /internal/orders/by-reservation/{reservationId}/confirm}.
     *
     * @throws OrdersApiException when the service did not accept it
     */
    void confirmByReservation(String reservationId, String correlationId);

    /**
     * Calls {@code POST /internal/orders/by-reservation/{reservationId}/release}.
     *
     * @throws OrdersApiException when the service did not accept it
     */
    void releaseByReservation(String reservationId, String correlationId);
}

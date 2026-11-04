package co.edu.corhuila.csp.worker.application.port.out;

/**
 * The Concessions service as seen by the worker: only the internal operations that settle its orders (ADR-045). The first two answer without error when the reservation has no order or the order is already in
 * that state, so a repeated message is harmless. The worker never touches the concessions schema.
 */
public interface ConcessionsOrdersApi {

    /**
     * Calls {@code POST /internal/orders/by-reservation/{reservationId}/confirm}. {@code eventId} is the
     * {@code eventId} of the Booking event, sent as {@code X-Event-Id} so a redelivery changes nothing (ADR-063); an
     * empty one sends no header.
     *
     * @throws OrdersApiException when the service did not accept it
     */
    void confirmByReservation(String reservationId, String eventId, String correlationId);

    /**
     * Calls {@code POST /internal/orders/by-reservation/{reservationId}/release}.
     *
     * @throws OrdersApiException when the service did not accept it
     */
    void releaseByReservation(String reservationId, String eventId, String correlationId);

    /**
     * Calls {@code POST /internal/orders/expire}: the service releases up to {@code batchSize} orders without a
     * reservation whose time ran out.
     *
     * @param batchSize how many orders the service may handle in this call, from 1 to 500
     * @return how many orders were released and whether more remain
     * @throws OrdersApiException when the service did not accept it
     */
    ExpireOrdersResult expireOrders(String correlationId, int batchSize);
}

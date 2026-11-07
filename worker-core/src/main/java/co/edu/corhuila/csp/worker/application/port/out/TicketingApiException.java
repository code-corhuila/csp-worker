package co.edu.corhuila.csp.worker.application.port.out;

/** The Ticketing service did not accept a call: the network, a failing service, a refused token or a refused request. */
public class TicketingApiException extends RuntimeException {

    public TicketingApiException(String message) {
        super(message);
    }

    public TicketingApiException(String message, Throwable cause) {
        super(message, cause);
    }
}

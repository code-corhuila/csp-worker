package co.edu.corhuila.csp.worker.application.port.out;

/**
 * The Concessions service did not accept a call. {@code retryable} says whether a later attempt can work: the
 * network, a busy or failing service, or a token that was refused (the secret of the worker is wrong, not the
 * message). A refusal of the message itself, such as a conflict between the order and the event, is not retryable.
 */
public class OrdersApiException extends RuntimeException {

    private final boolean retryable;

    public OrdersApiException(String message, boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    public OrdersApiException(String message, boolean retryable, Throwable cause) {
        super(message, cause);
        this.retryable = retryable;
    }

    public boolean retryable() {
        return retryable;
    }
}

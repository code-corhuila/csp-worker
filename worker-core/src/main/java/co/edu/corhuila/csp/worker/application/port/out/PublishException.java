package co.edu.corhuila.csp.worker.application.port.out;

/**
 * A publication the broker did not accept. A transient failure (connection lost, channel closed,
 * nack, confirm timeout) is retried; a permanent one (unroutable message) never is (ADR-014).
 */
public class PublishException extends RuntimeException {

    private final boolean permanent;

    private PublishException(String message, Throwable cause, boolean permanent) {
        super(message, cause);
        this.permanent = permanent;
    }

    public static PublishException transientFailure(String message, Throwable cause) {
        return new PublishException(message, cause, false);
    }

    public static PublishException permanent(String message, Throwable cause) {
        return new PublishException(message, cause, true);
    }

    public boolean isPermanent() {
        return permanent;
    }
}

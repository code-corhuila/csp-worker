package co.edu.corhuila.csp.worker.application.port.out;

/**
 * The outcome of one expiration sweep run, as reported by the booking service.
 */
public record ExpireHoldsResult(int expired, int remaining) {
}

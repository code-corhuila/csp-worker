package co.edu.corhuila.csp.worker.application.port.out;

/**
 * The outcome of one purge call, as reported by the identity service: how many rows it handled and
 * whether it left more for another call.
 */
public record PurgeResult(int processedCount, boolean hasMore) {
}

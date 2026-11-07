package co.edu.corhuila.csp.worker.application.port.out;

/** The outcome of one call of the sweep of orders, as reported by the concessions service. */
public record ExpireOrdersResult(int processedCount, boolean hasMore) {
}

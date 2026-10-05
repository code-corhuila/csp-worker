package co.edu.corhuila.csp.worker.application.port.in;

/**
 * The outcome of one job run: how many items were processed and how many failed. A failure in one
 * item does not stop the batch (Norma 5.7).
 */
public record JobResult(int processed, int failed) {
}

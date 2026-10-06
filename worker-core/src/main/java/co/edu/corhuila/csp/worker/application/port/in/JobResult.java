package co.edu.corhuila.csp.worker.application.port.in;

/**
 * The outcome of one job run. {@code processed} counts the items the run handled. {@code failed}
 * counts what could not be handled: an item the job gave up on, or {@code 1} when the whole run
 * failed before reaching any item (for example the service it calls was unreachable). A failure in
 * one item does not stop the batch (Norma 5.7).
 */
public record JobResult(int processed, int failed) {
}

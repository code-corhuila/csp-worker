package co.edu.corhuila.csp.worker.application.usecase;

import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;

/**
 * The run of a job that asks a service to handle its backlog one batch per call: it calls again while the
 * service says more remain and handled something, and stops when it runs out of time or calls. Every call of a
 * run carries the same correlation id so the run is traceable across the system (Norma 5.7.3). The idempotence
 * of each batch belongs to the service. Shared by {@link PurgeAuthJob} and {@link ExpireConcessionsOrdersJob}.
 */
final class DrainLoop {

    static final int MAX_CALLS_PER_RUN = 100;

    /** One call to the service: how many items it handled and whether it left more for another call. */
    record Progress(int processedCount, boolean hasMore) {
    }

    /** The call itself, with the correlation id of the run and the size of the batch. */
    @FunctionalInterface
    interface Batch {
        Progress call(String correlationId, int batchSize);
    }

    private DrainLoop() {
    }

    static JobResult run(String label, Batch batch, int batchSize, Duration runTimeout, Clock clock, Logger log) {
        String correlationId = UUID.randomUUID().toString();
        Instant deadline = clock.instant().plus(runTimeout);
        int processed = 0;
        int calls = 0;
        boolean hasMore = true;
        while (hasMore && calls < MAX_CALLS_PER_RUN && clock.instant().isBefore(deadline)) {
            calls++;
            Progress progress;
            try {
                progress = batch.call(correlationId, batchSize);
            } catch (Exception exception) {
                log.error("{} failed: processed={}, calls={}, correlationId={}", label, processed, calls,
                        correlationId, exception);
                return new JobResult(processed, 1);
            }
            processed += progress.processedCount();
            // A service that reports more but handled nothing would be called until the cap for no gain.
            hasMore = progress.hasMore() && progress.processedCount() > 0;
        }
        log.info("{} completed: processed={}, calls={}, moreRemain={}, correlationId={}", label, processed, calls,
                hasMore, correlationId);
        return new JobResult(processed, 0);
    }
}

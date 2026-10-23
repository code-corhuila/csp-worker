package co.edu.corhuila.csp.worker.application.usecase;

import co.edu.corhuila.csp.worker.application.port.in.Job;
import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.AuthMaintenanceApi;
import co.edu.corhuila.csp.worker.application.port.out.PurgeResult;
import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The purge of HU-AUTH-002 (ADR-030): asks the identity service to purge what it no longer needs,
 * one batch per call, until it has nothing more or the run is out of time. Every call of a run
 * carries the same correlation id so the run is traceable across the system (Norma 5.7.3).
 *
 * <p>Safe to run twice (Norma 5.7, rule 1): the idempotence of each batch belongs to the identity
 * service. A run is bounded by the batch size, its time and a cap of calls.
 */
public class PurgeAuthJob implements Job {

    /** The maximum batch size of the contract of {@code /internal/maintenance/purge}. */
    static final int MAX_BATCH_SIZE = 500;
    static final int MAX_CALLS_PER_RUN = DrainLoop.MAX_CALLS_PER_RUN;

    private static final Logger log = LoggerFactory.getLogger(PurgeAuthJob.class);

    private final AuthMaintenanceApi authApi;
    private final int batchSize;
    private final Duration runTimeout;
    private final Clock clock;

    public PurgeAuthJob(AuthMaintenanceApi authApi, int batchSize, Duration runTimeout, Clock clock) {
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("the purge batch size must be between 1 and " + MAX_BATCH_SIZE);
        }
        if (runTimeout.isZero() || runTimeout.isNegative()) {
            throw new IllegalArgumentException("the purge run timeout must be positive");
        }
        this.authApi = authApi;
        this.batchSize = batchSize;
        this.runTimeout = runTimeout;
        this.clock = clock;
    }

    @Override
    public JobResult run() {
        return DrainLoop.run("purge", (correlationId, size) -> {
            PurgeResult result = authApi.purge(correlationId, size);
            return new DrainLoop.Progress(result.processedCount(), result.hasMore());
        }, batchSize, runTimeout, clock, log);
    }

    @Override
    public String name() {
        return "purge-auth";
    }
}

package co.edu.corhuila.csp.worker.application.usecase;

import co.edu.corhuila.csp.worker.application.port.in.Job;
import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.ConcessionsOrdersApi;
import co.edu.corhuila.csp.worker.application.port.out.ExpireOrdersResult;
import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The sweep of the concession orders whose time ran out (HU-CONCESSIONS-004, ADR-045): an order without a
 * reservation keeps its stock only for a while, and nothing else will release it. Asks the concessions service
 * to release them, one batch per call, until it has nothing more or the run is out of time or calls.
 *
 * <p>Safe to run twice (Norma 5.7, rule 1): the service releases an order once, so a repeated call finds
 * nothing to do. A run is bounded by the batch size, its time and a cap of calls.
 */
public class ExpireConcessionsOrdersJob implements Job {

    /** The maximum batch size of the contract of {@code /internal/orders/expire}. */
    static final int MAX_BATCH_SIZE = 500;

    private static final Logger log = LoggerFactory.getLogger(ExpireConcessionsOrdersJob.class);

    private final ConcessionsOrdersApi ordersApi;
    private final int batchSize;
    private final Duration runTimeout;
    private final Clock clock;

    public ExpireConcessionsOrdersJob(ConcessionsOrdersApi ordersApi, int batchSize, Duration runTimeout, Clock clock) {
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("the sweep batch size must be between 1 and " + MAX_BATCH_SIZE);
        }
        if (runTimeout.isZero() || runTimeout.isNegative()) {
            throw new IllegalArgumentException("the sweep run timeout must be positive");
        }
        this.ordersApi = ordersApi;
        this.batchSize = batchSize;
        this.runTimeout = runTimeout;
        this.clock = clock;
    }

    @Override
    public JobResult run() {
        return DrainLoop.run("expire orders", (correlationId, size) -> {
            ExpireOrdersResult result = ordersApi.expireOrders(correlationId, size);
            return new DrainLoop.Progress(result.processedCount(), result.hasMore());
        }, batchSize, runTimeout, clock, log);
    }

    @Override
    public String name() {
        return "expire-concessions-orders";
    }
}

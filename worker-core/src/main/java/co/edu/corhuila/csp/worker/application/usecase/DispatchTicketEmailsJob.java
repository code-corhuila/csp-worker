package co.edu.corhuila.csp.worker.application.usecase;

import co.edu.corhuila.csp.worker.application.port.in.Job;
import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.TicketingDispatchApi;
import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The dispatch of the ticket emails (HU-TICKETING-002, ADR-057): the Ticketing service sends the email of a ticket
 * once its PDF exists and the contact of the user is known, and keeps a delivery pending, with a growing delay,
 * while the contact is not there yet. Nothing else starts that retry, so the worker asks the service to dispatch
 * what is due, one batch per call, until a batch comes back short or the run is out of time or calls.
 *
 * <p>Safe to run twice (Norma 5.7, rule 1): the service sends a delivery once and decides when a retry is due, so a
 * repeated call finds nothing to do. A run is bounded by the batch size, its time and a cap of calls.
 */
public class DispatchTicketEmailsJob implements Job {

    /** The maximum limit of the contract of {@code /internal/fulfillments/dispatch}. */
    static final int MAX_BATCH_SIZE = 100;

    private static final Logger log = LoggerFactory.getLogger(DispatchTicketEmailsJob.class);

    private final TicketingDispatchApi ticketing;
    private final int batchSize;
    private final Duration runTimeout;
    private final Clock clock;

    public DispatchTicketEmailsJob(TicketingDispatchApi ticketing, int batchSize, Duration runTimeout, Clock clock) {
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("the dispatch batch size must be between 1 and " + MAX_BATCH_SIZE);
        }
        if (runTimeout.isZero() || runTimeout.isNegative()) {
            throw new IllegalArgumentException("the dispatch run timeout must be positive");
        }
        this.ticketing = ticketing;
        this.batchSize = batchSize;
        this.runTimeout = runTimeout;
        this.clock = clock;
    }

    @Override
    public JobResult run() {
        return DrainLoop.run("dispatch ticket emails", (correlationId, size) -> {
            TicketingDispatchApi.Result result = ticketing.dispatch(correlationId, size);
            // A batch shorter than the limit means nothing else is due now.
            return new DrainLoop.Progress(result.handled(), result.handled() >= size);
        }, batchSize, runTimeout, clock, log);
    }

    @Override
    public String name() {
        return "dispatch-ticket-emails";
    }
}

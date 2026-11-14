package co.edu.corhuila.csp.worker.application.usecase;

import co.edu.corhuila.csp.worker.application.port.in.Job;
import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.TicketingHoldExpiryWarningsApi;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The warning of the holds that are about to expire (HU-TICKETING-005, scenario 2): asks the Ticketing service to
 * create the {@code HOLD_EXPIRY_WARNING} notifications that are due. It makes one bounded call per run; a failed
 * call is logged and the next run asks again, so nothing accumulates.
 *
 * <p>Safe to run twice (Norma 5.7, rule 1): the service creates one warning per hold, so overlapping runs, a
 * retried call or a second replica of this worker create it once. This job relies on it and does not coordinate
 * between replicas, like the other jobs that call a service.
 */
public class HoldExpiryWarningsJob implements Job {

    /** How many warnings one call may create. */
    static final int LIMIT = 100;

    private static final Logger log = LoggerFactory.getLogger(HoldExpiryWarningsJob.class);

    private final TicketingHoldExpiryWarningsApi ticketing;

    public HoldExpiryWarningsJob(TicketingHoldExpiryWarningsApi ticketing) {
        this.ticketing = ticketing;
    }

    @Override
    public JobResult run() {
        String correlationId = UUID.randomUUID().toString();
        try {
            int warned = ticketing.warn(correlationId, LIMIT);
            log.info("hold expiry warnings completed: warned={}, correlationId={}", warned, correlationId);
            return new JobResult(warned, 0);
        } catch (Exception exception) {
            log.error("hold expiry warnings failed: correlationId={}", correlationId, exception);
            return new JobResult(0, 1);
        }
    }

    @Override
    public String name() {
        return "hold-expiry-warnings";
    }
}

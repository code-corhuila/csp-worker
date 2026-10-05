package co.edu.corhuila.csp.worker.application.usecase;

import co.edu.corhuila.csp.worker.application.port.in.Job;
import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.BookingExpireHoldsApi;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The expiration sweep of HU-BOOKING-002: calls the booking service to expire HELD reservations
 * past their hold time. Each run carries its own correlation id so the sweep is traceable across
 * the whole system (Norma 5.7.3).
 */
public class ExpireHoldsJob implements Job {

    private static final Logger log = LoggerFactory.getLogger(ExpireHoldsJob.class);

    private final BookingExpireHoldsApi bookingApi;

    public ExpireHoldsJob(BookingExpireHoldsApi bookingApi) {
        this.bookingApi = bookingApi;
    }

    @Override
    public JobResult run() {
        String correlationId = UUID.randomUUID().toString();
        try {
            var result = bookingApi.expireHolds(correlationId);
            log.info("expiration sweep completed: expired={}, remaining={}, correlationId={}",
                    result.expired(), result.remaining(), correlationId);
            return new JobResult(result.expired(), 0);
        } catch (Exception exception) {
            log.error("expiration sweep failed: correlationId={}", correlationId, exception);
            return new JobResult(0, 1);
        }
    }

    @Override
    public String name() {
        return "expire-holds";
    }
}

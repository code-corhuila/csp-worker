package co.edu.corhuila.csp.worker.app;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import co.edu.corhuila.csp.worker.application.port.out.BookingExpireHoldsApi;
import co.edu.corhuila.csp.worker.application.usecase.ExpireHoldsJob;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The composition root: the only place that knows the concrete types. The core stays free of the
 * framework, so its jobs are registered here for the scheduler to find them.
 */
@Configuration
public class WorkerConfiguration {

    @Bean
    ScheduledJob expireHoldsJob(BookingExpireHoldsApi bookingApi,
            @Value("${worker.expire-holds.interval-seconds:60}") long intervalSeconds) {
        return new ScheduledJob(new ExpireHoldsJob(bookingApi), Duration.ofSeconds(intervalSeconds));
    }
}

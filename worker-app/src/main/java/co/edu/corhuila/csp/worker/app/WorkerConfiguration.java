package co.edu.corhuila.csp.worker.app;

import co.edu.corhuila.csp.worker.application.port.in.Job;
import co.edu.corhuila.csp.worker.application.port.out.BookingExpireHoldsApi;
import co.edu.corhuila.csp.worker.application.usecase.ExpireHoldsJob;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The composition root: the only place that knows the concrete types. The core stays free of the
 * framework, so its jobs are registered here for the scheduler to find them.
 */
@Configuration
public class WorkerConfiguration {

    @Bean
    Job expireHoldsJob(BookingExpireHoldsApi bookingApi) {
        return new ExpireHoldsJob(bookingApi);
    }
}

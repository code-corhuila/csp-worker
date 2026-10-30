package co.edu.corhuila.csp.worker.app;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import co.edu.corhuila.csp.worker.application.port.out.AuthMaintenanceApi;
import co.edu.corhuila.csp.worker.application.port.out.BookingExpireHoldsApi;
import co.edu.corhuila.csp.worker.application.port.out.ConcessionsOrdersApi;
import co.edu.corhuila.csp.worker.application.port.out.TicketingDispatchApi;
import co.edu.corhuila.csp.worker.application.usecase.DispatchTicketEmailsJob;
import co.edu.corhuila.csp.worker.application.usecase.ExpireConcessionsOrdersJob;
import co.edu.corhuila.csp.worker.application.usecase.ExpireHoldsJob;
import co.edu.corhuila.csp.worker.application.usecase.PurgeAuthJob;
import java.time.Clock;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The composition root: the only place that knows the concrete types. The core stays free of the
 * framework, so its jobs are registered here for the scheduler to find them.
 */
@Configuration
public class WorkerConfiguration {

    @Bean
    @ConditionalOnProperty(prefix = "worker.expire-holds", name = "enabled", havingValue = "true",
            matchIfMissing = true)
    ScheduledJob expireHoldsJob(BookingExpireHoldsApi bookingApi,
            @Value("${worker.expire-holds.interval-seconds:60}") long intervalSeconds) {
        return new ScheduledJob(new ExpireHoldsJob(bookingApi), Duration.ofSeconds(intervalSeconds));
    }

    @Bean
    @ConditionalOnProperty(prefix = "worker.purge-auth", name = "enabled", havingValue = "true")
    ScheduledJob purgeAuthJob(AuthMaintenanceApi authApi,
            @Value("${worker.purge-auth.batch-size:500}") int batchSize,
            @Value("${worker.purge-auth.run-timeout-seconds:60}") long runTimeoutSeconds,
            @Value("${worker.purge-auth.interval-seconds:3600}") long intervalSeconds) {
        return new ScheduledJob(
                new PurgeAuthJob(authApi, batchSize, Duration.ofSeconds(runTimeoutSeconds), Clock.systemUTC()),
                Duration.ofSeconds(intervalSeconds));
    }

    @Bean
    @ConditionalOnProperty(prefix = "worker.expire-concessions-orders", name = "enabled", havingValue = "true")
    ScheduledJob expireConcessionsOrdersJob(ConcessionsOrdersApi ordersApi,
            @Value("${worker.expire-concessions-orders.batch-size:100}") int batchSize,
            @Value("${worker.expire-concessions-orders.run-timeout-seconds:60}") long runTimeoutSeconds,
            @Value("${worker.expire-concessions-orders.interval-seconds:60}") long intervalSeconds) {
        return new ScheduledJob(new ExpireConcessionsOrdersJob(ordersApi, batchSize,
                Duration.ofSeconds(runTimeoutSeconds), Clock.systemUTC()), Duration.ofSeconds(intervalSeconds));
    }

    @Bean
    @ConditionalOnProperty(prefix = "worker.dispatch-ticket-emails", name = "enabled", havingValue = "true")
    ScheduledJob dispatchTicketEmailsJob(TicketingDispatchApi ticketingApi,
            @Value("${worker.dispatch-ticket-emails.batch-size:50}") int batchSize,
            @Value("${worker.dispatch-ticket-emails.run-timeout-seconds:60}") long runTimeoutSeconds,
            @Value("${worker.dispatch-ticket-emails.interval-seconds:30}") long intervalSeconds) {
        return new ScheduledJob(new DispatchTicketEmailsJob(ticketingApi, batchSize,
                Duration.ofSeconds(runTimeoutSeconds), Clock.systemUTC()), Duration.ofSeconds(intervalSeconds));
    }
}

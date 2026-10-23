package co.edu.corhuila.csp.worker.app;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import co.edu.corhuila.csp.worker.adapter.out.amqp.AmqpReservationEvents;
import co.edu.corhuila.csp.worker.application.port.out.ConcessionsOrdersApi;
import co.edu.corhuila.csp.worker.application.usecase.ReservationEventsJob;
import com.rabbitmq.client.ConnectionFactory;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The job that settles the concession order of a reservation (HU-CONCESSIONS-004, ADR-045), composed only when
 * {@code worker.concessions-reservation-events.enabled} is true. It reads the queues of Concessions with the
 * broker login of that service ({@code concessions}, ADR-036), not with the publishing login of the relay, and
 * calls the Concessions API with the service token. Enabled without a broker, it stops at startup.
 */
@Configuration
@ConditionalOnProperty(prefix = "worker.concessions-reservation-events", name = "enabled", havingValue = "true")
public class ConcessionsReservationEventsConfiguration {

    @Bean(destroyMethod = "close")
    AmqpReservationEvents reservationEventQueue(
            @Value("${worker.concessions-reservation-events.amqp.url:}") String url,
            @Value("${worker.concessions-reservation-events.amqp.exchange:cine.events}") String exchange,
            @Value("${worker.concessions-reservation-events.amqp.dead-letter-exchange:cine.dlx}") String deadLetterExchange)
            throws Exception {
        if (url == null || url.isBlank()) {
            throw new IllegalStateException("CONCESSIONS_AMQP_URL is required to settle the orders of the reservations");
        }
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(URI.create(url));
        factory.setConnectionTimeout(5_000);
        return new AmqpReservationEvents(factory, exchange, deadLetterExchange);
    }

    @Bean
    ScheduledJob concessionsReservationEventsJob(AmqpReservationEvents reservationEventQueue,
            ConcessionsOrdersApi concessionsApi,
            @Value("${worker.concessions-reservation-events.batch-size:100}") int batchSize,
            @Value("${worker.concessions-reservation-events.run-timeout-seconds:20}") long runTimeoutSeconds,
            @Value("${worker.concessions-reservation-events.interval-seconds:5}") long intervalSeconds) {
        return new ScheduledJob(new ReservationEventsJob(reservationEventQueue, concessionsApi, batchSize,
                Duration.ofSeconds(runTimeoutSeconds), Clock.systemUTC()), Duration.ofSeconds(intervalSeconds));
    }
}

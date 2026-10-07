package co.edu.corhuila.csp.worker.app;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import co.edu.corhuila.csp.worker.adapter.out.amqp.AmqpEventPublisher;
import co.edu.corhuila.csp.worker.adapter.out.outbox.EnvelopeSchemaValidator;
import co.edu.corhuila.csp.worker.adapter.out.outbox.JdbcOutboxSource;
import co.edu.corhuila.csp.worker.adapter.out.outbox.JdbcRelayStateStore;
import co.edu.corhuila.csp.worker.application.usecase.RelayOutboxJob;
import co.edu.corhuila.csp.worker.application.usecase.RetryPolicy;
import com.rabbitmq.client.ConnectionFactory;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The outbox relay of booking (ADR-014), composed only when {@code worker.outbox-relay.enabled} is
 * true: it needs the {@code worker_app} login of the PostgreSQL instance and a RabbitMQ broker, and
 * a worker that has neither keeps running its other jobs. Enabled without them, it stops at
 * startup instead of failing at the first run.
 */
@Configuration
@ConditionalOnProperty(prefix = "worker.outbox-relay", name = "enabled", havingValue = "true")
public class OutboxRelayConfiguration {

    @Bean(destroyMethod = "close")
    HikariDataSource relayDataSource(
            @Value("${worker.outbox-relay.database.url:}") String url,
            @Value("${worker.outbox-relay.database.username:worker_app}") String username,
            @Value("${worker.outbox-relay.database.password:}") String password) {
        require(url, "RELAY_DATABASE_URL");
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaximumPoolSize(2);
        dataSource.setPoolName("outbox-relay");
        return dataSource;
    }

    @Bean(destroyMethod = "close")
    AmqpEventPublisher relayPublisher(
            @Value("${worker.outbox-relay.amqp.url:}") String url,
            @Value("${worker.outbox-relay.amqp.exchange:cine.events}") String exchange,
            @Value("${worker.outbox-relay.amqp.confirm-timeout-seconds:5}") long confirmTimeoutSeconds)
            throws Exception {
        require(url, "RELAY_AMQP_URL");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(URI.create(url));
        factory.setConnectionTimeout(5_000);
        return new AmqpEventPublisher(factory, exchange, Duration.ofSeconds(confirmTimeoutSeconds));
    }

    @Bean
    ScheduledJob outboxRelayJob(HikariDataSource relayDataSource, AmqpEventPublisher relayPublisher,
            @Value("${worker.outbox-relay.interval-seconds:5}") long intervalSeconds) {
        JdbcTemplate jdbc = new JdbcTemplate(relayDataSource);
        RelayOutboxJob job = new RelayOutboxJob(new JdbcOutboxSource(jdbc), new JdbcRelayStateStore(jdbc),
                new EnvelopeSchemaValidator(), relayPublisher,
                new RetryPolicy(() -> ThreadLocalRandom.current().nextDouble()), Clock.systemUTC());
        return new ScheduledJob(job, Duration.ofSeconds(intervalSeconds));
    }

    private static void require(String value, String variable) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(variable + " is required when the outbox relay is enabled");
        }
    }
}

package co.edu.corhuila.csp.worker.app;

import static co.edu.corhuila.csp.worker.app.OutboxRelayConfiguration.require;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import co.edu.corhuila.csp.worker.adapter.out.amqp.AmqpEventPublisher;
import co.edu.corhuila.csp.worker.adapter.out.outbox.EnvelopeSchemaValidator;
import co.edu.corhuila.csp.worker.adapter.out.outbox.JdbcAdvisoryRunLock;
import co.edu.corhuila.csp.worker.adapter.out.outbox.JdbcOutboxSource;
import co.edu.corhuila.csp.worker.adapter.out.outbox.JdbcRelayStateStore;
import co.edu.corhuila.csp.worker.adapter.out.outbox.RelayDomain;
import co.edu.corhuila.csp.worker.application.usecase.RelayOutboxJob;
import co.edu.corhuila.csp.worker.application.usecase.RetryPolicy;
import com.rabbitmq.client.ConnectionFactory;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * The outbox relay of Concessions (ADR-019, ADR-045), composed only when {@code worker.concessions-outbox-relay.enabled}
 * is true. It uses the same {@code worker_app} login and broker as the other relays, its own state key, lock and
 * connection, and publishes only the event types of {@code worker.concessions-outbox-relay.event-types}: the ones
 * that have a consumer. Enabled without a database or a broker, it stops at startup.
 */
@Configuration
@ConditionalOnProperty(prefix = "worker.concessions-outbox-relay", name = "enabled", havingValue = "true")
public class ConcessionsOutboxRelayConfiguration {

    /** The event types of {@code events.md}; any other name in the setting is a typo and stops the startup. */
    static final Set<String> KNOWN_EVENT_TYPES = Set.of("ConcessionOrderCreated", "ConcessionStockReserved",
            "ConcessionOrderConfirmed", "ConcessionOrderCancelled");

    @Bean(destroyMethod = "close")
    HikariDataSource concessionsRelayDataSource(
            @Value("${worker.outbox-relay.database.url:}") String url,
            @Value("${worker.outbox-relay.database.username:worker_app}") String username,
            @Value("${worker.outbox-relay.database.password:}") String password) {
        require(url, "RELAY_DATABASE_URL");
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaximumPoolSize(2);
        dataSource.setPoolName("concessions-outbox-relay");
        return dataSource;
    }

    @Bean(destroyMethod = "close")
    AmqpEventPublisher concessionsRelayPublisher(
            @Value("${worker.outbox-relay.amqp.url:}") String url,
            @Value("${worker.outbox-relay.amqp.exchange:cine.events}") String exchange,
            @Value("${worker.outbox-relay.amqp.confirm-timeout-seconds:5}") long confirmTimeoutSeconds)
            throws Exception {
        require(url, "RELAY_AMQP_URL");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(URI.create(url));
        factory.setConnectionTimeout(5_000);
        return new AmqpEventPublisher(factory, exchange, Duration.ofSeconds(confirmTimeoutSeconds), RelayDomain.CONCESSIONS);
    }

    /** Not pooled on purpose: the lock lives as long as its connection (see {@link JdbcAdvisoryRunLock}). */
    @Bean
    JdbcAdvisoryRunLock concessionsRelayLock(
            @Value("${worker.outbox-relay.database.url:}") String url,
            @Value("${worker.outbox-relay.database.username:worker_app}") String username,
            @Value("${worker.outbox-relay.database.password:}") String password) {
        return new JdbcAdvisoryRunLock(new DriverManagerDataSource(url, username, password), RelayDomain.CONCESSIONS);
    }

    @Bean
    ScheduledJob concessionsOutboxRelayJob(HikariDataSource concessionsRelayDataSource, AmqpEventPublisher concessionsRelayPublisher,
            JdbcAdvisoryRunLock concessionsRelayLock,
            @Value("${worker.concessions-outbox-relay.interval-seconds:5}") long intervalSeconds,
            @Value("${worker.concessions-outbox-relay.event-types:ConcessionOrderConfirmed}") String eventTypes) {
        List<String> published = Arrays.stream(eventTypes.split(",")).map(String::trim).filter(type -> !type.isEmpty()).toList();
        if (published.isEmpty() || !KNOWN_EVENT_TYPES.containsAll(published)) {
            throw new IllegalStateException("CONCESSIONS_OUTBOX_RELAY_EVENT_TYPES must list at least one of "
                    + KNOWN_EVENT_TYPES + ", it has " + eventTypes);
        }
        JdbcTemplate jdbc = new JdbcTemplate(concessionsRelayDataSource);
        RelayOutboxJob job = new RelayOutboxJob("concessions-outbox-relay",
                new JdbcOutboxSource(jdbc, RelayDomain.CONCESSIONS, published),
                new JdbcRelayStateStore(jdbc, RelayDomain.CONCESSIONS),
                new EnvelopeSchemaValidator(RelayDomain.CONCESSIONS), concessionsRelayPublisher,
                new RetryPolicy(() -> ThreadLocalRandom.current().nextDouble()), Clock.systemUTC(), concessionsRelayLock);
        return new ScheduledJob(job, Duration.ofSeconds(intervalSeconds));
    }
}

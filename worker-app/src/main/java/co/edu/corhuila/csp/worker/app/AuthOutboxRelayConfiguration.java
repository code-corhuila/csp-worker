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
 * The outbox relay of auth (ADR-019), composed only when {@code worker.auth-outbox-relay.enabled} is
 * true. It uses the same {@code worker_app} login and broker as the booking relay, its own state key,
 * lock and connection, and publishes only the event types of {@code worker.auth-outbox-relay.event-types}:
 * the ones that have a consumer. Enabled without a database or a broker, it stops at startup.
 */
@Configuration
@ConditionalOnProperty(prefix = "worker.auth-outbox-relay", name = "enabled", havingValue = "true")
public class AuthOutboxRelayConfiguration {

    /** The event types of {@code events.md}; any other name in the setting is a typo and stops the startup. */
    static final Set<String> KNOWN_EVENT_TYPES = Set.of("UserRegistered", "UserEmailChanged", "UserRoleChanged");

    @Bean(destroyMethod = "close")
    HikariDataSource authRelayDataSource(
            @Value("${worker.outbox-relay.database.url:}") String url,
            @Value("${worker.outbox-relay.database.username:worker_app}") String username,
            @Value("${worker.outbox-relay.database.password:}") String password) {
        require(url, "RELAY_DATABASE_URL");
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaximumPoolSize(2);
        dataSource.setPoolName("auth-outbox-relay");
        return dataSource;
    }

    @Bean(destroyMethod = "close")
    AmqpEventPublisher authRelayPublisher(
            @Value("${worker.outbox-relay.amqp.url:}") String url,
            @Value("${worker.outbox-relay.amqp.exchange:cine.events}") String exchange,
            @Value("${worker.outbox-relay.amqp.confirm-timeout-seconds:5}") long confirmTimeoutSeconds)
            throws Exception {
        require(url, "RELAY_AMQP_URL");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(URI.create(url));
        factory.setConnectionTimeout(5_000);
        return new AmqpEventPublisher(factory, exchange, Duration.ofSeconds(confirmTimeoutSeconds), RelayDomain.AUTH);
    }

    /** Not pooled on purpose: the lock lives as long as its connection (see {@link JdbcAdvisoryRunLock}). */
    @Bean
    JdbcAdvisoryRunLock authRelayLock(
            @Value("${worker.outbox-relay.database.url:}") String url,
            @Value("${worker.outbox-relay.database.username:worker_app}") String username,
            @Value("${worker.outbox-relay.database.password:}") String password) {
        return new JdbcAdvisoryRunLock(new DriverManagerDataSource(url, username, password), RelayDomain.AUTH);
    }

    @Bean
    ScheduledJob authOutboxRelayJob(HikariDataSource authRelayDataSource, AmqpEventPublisher authRelayPublisher,
            JdbcAdvisoryRunLock authRelayLock,
            @Value("${worker.auth-outbox-relay.interval-seconds:5}") long intervalSeconds,
            @Value("${worker.auth-outbox-relay.event-types:UserRegistered,UserEmailChanged}") String eventTypes) {
        List<String> published = Arrays.stream(eventTypes.split(",")).map(String::trim).filter(type -> !type.isEmpty()).toList();
        if (published.isEmpty() || !KNOWN_EVENT_TYPES.containsAll(published)) {
            throw new IllegalStateException("AUTH_OUTBOX_RELAY_EVENT_TYPES must list at least one of "
                    + KNOWN_EVENT_TYPES + ", it has " + eventTypes);
        }
        JdbcTemplate jdbc = new JdbcTemplate(authRelayDataSource);
        RelayOutboxJob job = new RelayOutboxJob("auth-outbox-relay",
                new JdbcOutboxSource(jdbc, RelayDomain.AUTH, published),
                new JdbcRelayStateStore(jdbc, RelayDomain.AUTH),
                new EnvelopeSchemaValidator(RelayDomain.AUTH), authRelayPublisher,
                new RetryPolicy(() -> ThreadLocalRandom.current().nextDouble()), Clock.systemUTC(), authRelayLock);
        return new ScheduledJob(job, Duration.ofSeconds(intervalSeconds));
    }
}

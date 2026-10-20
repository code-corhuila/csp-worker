package co.edu.corhuila.csp.worker.adapter.out.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import co.edu.corhuila.csp.worker.adapter.out.amqp.AmqpEventPublisher;
import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.usecase.RelayOutboxJob;
import co.edu.corhuila.csp.worker.application.usecase.RetryPolicy;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * The relay of the auth outbox with nothing replaced: rows of a real PostgreSQL, the state in schema
 * worker and a real RabbitMQ with the queues a consumer binds by routing key (ADR-019, ADR-036).
 */
@EnabledIfEnvironmentVariable(named = "TEST_DATABASE_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "TEST_AMQP_URL", matches = ".+")
class AuthOutboxRelayEndToEndTest {

    private static final String EXCHANGE = "cine.events.auth-e2e";
    private static final List<String> PUBLISHED_TYPES = List.of("UserRegistered", "UserEmailChanged");

    private JdbcTemplate jdbc;
    private Connection consumerConnection;
    private Channel consumer;
    private AmqpEventPublisher publisher;
    private RelayOutboxJob job;

    @BeforeEach
    void setUp() throws Exception {
        jdbc = PostgresSupport.freshDatabase();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(URI.create(System.getenv("TEST_AMQP_URL")));
        factory.setConnectionTimeout(2_000);
        consumerConnection = factory.newConnection("auth-e2e-consumer");
        consumer = consumerConnection.createChannel();
        consumer.exchangeDeclare(EXCHANGE, "topic", true);
        consumer.queueDeclare("relay.auth.e2e", false, false, true, null);
        consumer.queuePurge("relay.auth.e2e");
        consumer.queueBind("relay.auth.e2e", EXCHANGE, "cine.auth.user-registered.v1");
        consumer.queueBind("relay.auth.e2e", EXCHANGE, "cine.auth.user-email-changed.v1");
        publisher = new AmqpEventPublisher(factory, EXCHANGE, Duration.ofSeconds(5), RelayDomain.AUTH);
        job = relay(PUBLISHED_TYPES);
    }

    @AfterEach
    void tearDown() throws Exception {
        publisher.close();
        consumer.exchangeDelete(EXCHANGE);
        consumerConnection.close();
    }

    private RelayOutboxJob relay(List<String> eventTypes) {
        return new RelayOutboxJob("auth-outbox-relay", new JdbcOutboxSource(jdbc, RelayDomain.AUTH, eventTypes),
                new JdbcRelayStateStore(jdbc, RelayDomain.AUTH), new EnvelopeSchemaValidator(RelayDomain.AUTH),
                publisher, new RetryPolicy(() -> 0.5), Clock.systemUTC(),
                new JdbcAdvisoryRunLock(new DriverManagerDataSource(System.getenv("TEST_DATABASE_URL")),
                        RelayDomain.AUTH));
    }

    private String status(UUID id) {
        return jdbc.queryForObject("SELECT status FROM worker.outbox_relay_state WHERE event_id = ?",
                String.class, id);
    }

    @Test
    void theEventsWithAConsumerReachTheBrokerWithTheirRoutingKeyAndAreRecordedAsPublished() throws Exception {
        UUID registered = PostgresSupport.insertAuthEvent(jdbc, "UserRegistered", "2026-10-07T12:00:00Z");
        UUID emailChanged = PostgresSupport.insertAuthEvent(jdbc, "UserEmailChanged", "2026-10-07T12:00:01Z");

        JobResult result = job.run();

        assertEquals(2, result.processed());
        GetResponse first = consumer.basicGet("relay.auth.e2e", true);
        GetResponse second = consumer.basicGet("relay.auth.e2e", true);
        assertNotNull(first);
        assertNotNull(second);
        assertEquals("cine.auth.user-registered.v1", first.getEnvelope().getRoutingKey());
        assertEquals(registered.toString(), first.getProps().getMessageId());
        assertEquals("cine.auth.user-email-changed.v1", second.getEnvelope().getRoutingKey());
        assertEquals(emailChanged.toString(), second.getProps().getMessageId());
        assertEquals("PUBLISHED", status(registered));
        assertEquals("auth", jdbc.queryForObject("SELECT source FROM worker.outbox_relay_state WHERE event_id = ?",
                String.class, registered));
        assertEquals(0, job.run().processed());
        assertNull(consumer.basicGet("relay.auth.e2e", true));
    }

    @Test
    void anEventTypeThatIsNotPublishedIsSkippedAndNeverMarkedFailed() throws Exception {
        UUID roleChanged = PostgresSupport.insertAuthEvent(jdbc, "UserRoleChanged", "2026-10-07T12:00:00Z");

        JobResult result = job.run();

        assertEquals(0, result.processed());
        assertEquals(0, result.failed());
        assertNull(consumer.basicGet("relay.auth.e2e", true));
        assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM worker.outbox_relay_state WHERE event_id = ?",
                Long.class, roleChanged));
    }

    @Test
    void anEventTypeEnabledLaterIsPublishedFromThenOn() throws Exception {
        consumer.queueBind("relay.auth.e2e", EXCHANGE, "cine.auth.user-role-changed.v1");
        job.run();
        UUID roleChanged = PostgresSupport.insertAuthEvent(jdbc, "UserRoleChanged", "2099-01-01T00:00:00Z");

        RelayOutboxJob withRoles = relay(List.of("UserRegistered", "UserEmailChanged", "UserRoleChanged"));
        assertEquals(1, withRoles.run().processed());

        assertEquals("cine.auth.user-role-changed.v1", consumer.basicGet("relay.auth.e2e", true).getEnvelope().getRoutingKey());
        assertEquals("PUBLISHED", status(roleChanged));
    }

    @Test
    void aMessageNoQueueIsBoundToFailsForGoodAndIsNotRetried() throws Exception {
        consumer.queueUnbind("relay.auth.e2e", EXCHANGE, "cine.auth.user-registered.v1");
        UUID registered = PostgresSupport.insertAuthEvent(jdbc, "UserRegistered", "2026-10-07T12:00:00Z");

        JobResult result = job.run();

        assertEquals(1, result.failed());
        assertEquals("FAILED_PERMANENT", status(registered));
        assertEquals(0, job.run().failed());
    }

    @Test
    void theAuthOutboxIsLeftUntouchedAndTheBookingOutboxIsNotRead() throws Exception {
        UUID registered = PostgresSupport.insertAuthEvent(jdbc, "UserRegistered", "2026-10-07T12:00:00Z");
        PostgresSupport.insertEvent(jdbc, UUID.randomUUID(), "UserRegistered", "2026-10-07T12:00:01Z", "booking-row");

        assertEquals(1, job.run().processed());

        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM auth.outbox_event WHERE id = ?", Long.class, registered));
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM worker.outbox_relay_state", Long.class));
    }
}

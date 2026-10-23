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
 * The relay of the Concessions outbox with nothing replaced: rows of a real PostgreSQL, the state in schema
 * worker and a real RabbitMQ with the queue a consumer binds by routing key (ADR-019, ADR-036, ADR-045).
 */
@EnabledIfEnvironmentVariable(named = "TEST_DATABASE_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "TEST_AMQP_URL", matches = ".+")
class ConcessionsOutboxRelayEndToEndTest {

    private static final String EXCHANGE = "cine.events.concessions-e2e";
    private static final String QUEUE = "relay.concessions.e2e";
    private static final List<String> PUBLISHED_TYPES = List.of("ConcessionOrderConfirmed");

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
        consumerConnection = factory.newConnection("concessions-e2e-consumer");
        consumer = consumerConnection.createChannel();
        consumer.exchangeDeclare(EXCHANGE, "topic", true);
        consumer.queueDeclare(QUEUE, false, false, true, null);
        consumer.queuePurge(QUEUE);
        consumer.queueBind(QUEUE, EXCHANGE, "cine.concessions.order-confirmed.v1");
        publisher = new AmqpEventPublisher(factory, EXCHANGE, Duration.ofSeconds(5), RelayDomain.CONCESSIONS);
        job = relay(PUBLISHED_TYPES);
    }

    @AfterEach
    void tearDown() throws Exception {
        publisher.close();
        consumer.exchangeDelete(EXCHANGE);
        consumerConnection.close();
    }

    private RelayOutboxJob relay(List<String> eventTypes) {
        return new RelayOutboxJob("concessions-outbox-relay",
                new JdbcOutboxSource(jdbc, RelayDomain.CONCESSIONS, eventTypes),
                new JdbcRelayStateStore(jdbc, RelayDomain.CONCESSIONS),
                new EnvelopeSchemaValidator(RelayDomain.CONCESSIONS), publisher, new RetryPolicy(() -> 0.5),
                Clock.systemUTC(),
                new JdbcAdvisoryRunLock(new DriverManagerDataSource(System.getenv("TEST_DATABASE_URL")),
                        RelayDomain.CONCESSIONS));
    }

    private String status(UUID id) {
        return jdbc.queryForObject("SELECT status FROM worker.outbox_relay_state WHERE event_id = ?", String.class, id);
    }

    @Test
    void anOrderConfirmationReachesTheBrokerWithItsRoutingKeyAndIsRecordedAsPublished() throws Exception {
        UUID confirmed = PostgresSupport.insertConcessionsEvent(jdbc, "ConcessionOrderConfirmed", "2026-10-22T12:00:00Z");

        JobResult result = job.run();

        assertEquals(1, result.processed());
        GetResponse message = consumer.basicGet(QUEUE, true);
        assertNotNull(message);
        assertEquals("cine.concessions.order-confirmed.v1", message.getEnvelope().getRoutingKey());
        assertEquals(confirmed.toString(), message.getProps().getMessageId());
        assertEquals("corr-1", String.valueOf(message.getProps().getHeaders().get("X-Correlation-Id")));
        assertEquals("PUBLISHED", status(confirmed));
        assertEquals("concessions", jdbc.queryForObject(
                "SELECT source FROM worker.outbox_relay_state WHERE event_id = ?", String.class, confirmed));
        assertEquals(0, job.run().processed());
        assertNull(consumer.basicGet(QUEUE, true));
    }

    @Test
    void anEventTypeThatIsNotPublishedIsSkippedAndNeverMarkedFailed() throws Exception {
        UUID created = PostgresSupport.insertConcessionsEvent(jdbc, "ConcessionOrderCreated", "2026-10-22T12:00:00Z");

        JobResult result = job.run();

        assertEquals(0, result.processed());
        assertEquals(0, result.failed());
        assertNull(consumer.basicGet(QUEUE, true));
        assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM worker.outbox_relay_state WHERE event_id = ?",
                Long.class, created));
    }

    @Test
    void aMessageNoQueueIsBoundToFailsForGoodAndIsNotRetried() throws Exception {
        consumer.queueUnbind(QUEUE, EXCHANGE, "cine.concessions.order-confirmed.v1");
        UUID confirmed = PostgresSupport.insertConcessionsEvent(jdbc, "ConcessionOrderConfirmed", "2026-10-22T12:00:00Z");

        JobResult result = job.run();

        assertEquals(1, result.failed());
        assertEquals("FAILED_PERMANENT", status(confirmed));
        assertEquals(0, job.run().failed());
    }

    @Test
    void theConcessionsOutboxIsLeftUntouchedAndTheOtherOutboxesAreNotRead() throws Exception {
        UUID confirmed = PostgresSupport.insertConcessionsEvent(jdbc, "ConcessionOrderConfirmed", "2026-10-22T12:00:00Z");
        PostgresSupport.insertAuthEvent(jdbc, "UserRegistered", "2026-10-22T12:00:01Z");
        PostgresSupport.insertEvent(jdbc, UUID.randomUUID(), "ConcessionOrderConfirmed", "2026-10-22T12:00:02Z", "booking-row");

        assertEquals(1, job.run().processed());

        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM concessions.outbox_event WHERE id = ?", Long.class,
                confirmed));
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM worker.outbox_relay_state", Long.class));
    }
}

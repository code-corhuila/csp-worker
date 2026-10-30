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
 * The relay of the Ticketing outbox with nothing replaced: rows of a real PostgreSQL, the state in schema
 * worker and a real RabbitMQ with the queue a consumer binds by routing key (ADR-019, ADR-036, ADR-057).
 */
@EnabledIfEnvironmentVariable(named = "TEST_DATABASE_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "TEST_AMQP_URL", matches = ".+")
class TicketingOutboxRelayEndToEndTest {

    private static final String EXCHANGE = "cine.events.ticketing-e2e";
    private static final String QUEUE = "relay.ticketing.e2e";
    private static final String ROUTING_KEY = "cine.ticketing_fulfillment.ticket-issued.v1";

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
        consumerConnection = factory.newConnection("ticketing-e2e-consumer");
        consumer = consumerConnection.createChannel();
        consumer.exchangeDeclare(EXCHANGE, "topic", true);
        consumer.queueDeclare(QUEUE, false, false, true, null);
        consumer.queuePurge(QUEUE);
        consumer.queueBind(QUEUE, EXCHANGE, ROUTING_KEY);
        publisher = new AmqpEventPublisher(factory, EXCHANGE, Duration.ofSeconds(5), RelayDomain.TICKETING);
        job = new RelayOutboxJob("ticketing-outbox-relay",
                new JdbcOutboxSource(jdbc, RelayDomain.TICKETING, List.of("TicketIssued")),
                new JdbcRelayStateStore(jdbc, RelayDomain.TICKETING),
                new EnvelopeSchemaValidator(RelayDomain.TICKETING), publisher, new RetryPolicy(() -> 0.5),
                Clock.systemUTC(),
                new JdbcAdvisoryRunLock(new DriverManagerDataSource(System.getenv("TEST_DATABASE_URL")),
                        RelayDomain.TICKETING));
    }

    @AfterEach
    void tearDown() throws Exception {
        publisher.close();
        consumer.exchangeDelete(EXCHANGE);
        consumerConnection.close();
    }

    private String status(UUID id) {
        return jdbc.queryForObject("SELECT status FROM worker.outbox_relay_state WHERE event_id = ?", String.class, id);
    }

    @Test
    void aTicketIssuedReachesTheBrokerWithItsRoutingKeyAndIsRecordedAsPublished() throws Exception {
        UUID issued = PostgresSupport.insertTicketingEvent(jdbc, "TicketIssued", "2026-11-01T12:00:00Z");

        JobResult result = job.run();

        assertEquals(1, result.processed());
        GetResponse message = consumer.basicGet(QUEUE, true);
        assertNotNull(message);
        assertEquals(ROUTING_KEY, message.getEnvelope().getRoutingKey());
        assertEquals(issued.toString(), message.getProps().getMessageId());
        assertEquals("corr-1", String.valueOf(message.getProps().getHeaders().get("X-Correlation-Id")));
        assertEquals("PUBLISHED", status(issued));
        assertEquals("ticketing_fulfillment", jdbc.queryForObject(
                "SELECT source FROM worker.outbox_relay_state WHERE event_id = ?", String.class, issued));
        assertEquals(0, job.run().processed());
        assertNull(consumer.basicGet(QUEUE, true));
    }

    @Test
    void aMessageNoQueueIsBoundToFailsForGoodAndIsNotRetried() throws Exception {
        consumer.queueUnbind(QUEUE, EXCHANGE, ROUTING_KEY);
        UUID issued = PostgresSupport.insertTicketingEvent(jdbc, "TicketIssued", "2026-11-01T12:00:00Z");

        JobResult result = job.run();

        assertEquals(1, result.failed());
        assertEquals("FAILED_PERMANENT", status(issued));
        assertEquals(0, job.run().failed());
    }

    @Test
    void theTicketingOutboxIsLeftUntouchedAndTheOtherOutboxesAreNotRead() throws Exception {
        UUID issued = PostgresSupport.insertTicketingEvent(jdbc, "TicketIssued", "2026-11-01T12:00:00Z");
        PostgresSupport.insertConcessionsEvent(jdbc, "ConcessionOrderConfirmed", "2026-11-01T12:00:01Z");
        PostgresSupport.insertEvent(jdbc, UUID.randomUUID(), "TicketIssued", "2026-11-01T12:00:02Z", "booking-row");

        assertEquals(1, job.run().processed());

        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM ticketing_fulfillment.outbox_event WHERE id = ?",
                Long.class, issued));
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM worker.outbox_relay_state", Long.class));
    }
}

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
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The whole relay with nothing replaced: rows of a real PostgreSQL outbox, the state in schema
 * worker and a real RabbitMQ. It is the evidence behind criteria 4, 6 and 8 of ADR-014 that the
 * pieces tested one by one also work together.
 */
@EnabledIfEnvironmentVariable(named = "TEST_DATABASE_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "TEST_AMQP_URL", matches = ".+")
class OutboxRelayEndToEndTest {

    private static final String EXCHANGE = "cine.events.e2e";

    private JdbcTemplate jdbc;
    private ConnectionFactory factory;
    private Connection consumerConnection;
    private Channel consumer;
    private AmqpEventPublisher publisher;
    private RelayOutboxJob job;

    @BeforeEach
    void setUp() throws Exception {
        jdbc = PostgresSupport.freshDatabase();
        factory = new ConnectionFactory();
        factory.setUri(URI.create(System.getenv("TEST_AMQP_URL")));
        factory.setConnectionTimeout(2_000);
        consumerConnection = factory.newConnection("e2e-consumer");
        consumer = consumerConnection.createChannel();
        consumer.exchangeDeclare(EXCHANGE, "topic", true);
        consumer.queueDeclare("relay.e2e", false, false, true, null);
        consumer.queuePurge("relay.e2e");
        consumer.queueBind("relay.e2e", EXCHANGE, "cine.booking.reservation-held.v1");
        publisher = new AmqpEventPublisher(factory, EXCHANGE, Duration.ofSeconds(5));
        job = new RelayOutboxJob(new JdbcOutboxSource(jdbc), new JdbcRelayStateStore(jdbc),
                new EnvelopeSchemaValidator(), publisher, new RetryPolicy(() -> 0.5), Clock.systemUTC());
    }

    @AfterEach
    void tearDown() throws Exception {
        publisher.close();
        consumer.exchangeDelete(EXCHANGE);
        consumerConnection.close();
    }

    private UUID insertHeld(String createdAt) {
        UUID id = UUID.randomUUID();
        UUID aggregate = UUID.randomUUID();
        String envelope = "{\"eventId\":\"" + id + "\",\"eventType\":\"ReservationHeld\","
                + "\"occurredAt\":\"" + createdAt + "\",\"version\":1,\"source\":\"booking-service\","
                + "\"aggregateId\":\"" + aggregate + "\",\"aggregateType\":\"Reservation\","
                + "\"payload\":{\"reservationId\":\"" + aggregate + "\"},"
                + "\"metadata\":{\"correlationId\":\"corr-e2e\",\"causationId\":null}}";
        jdbc.update("INSERT INTO booking.outbox_event (id, aggregate_type, aggregate_id, event_type, payload, "
                + "created_at) VALUES (?, 'Reservation', ?, 'ReservationHeld', ?::jsonb, ?::timestamptz)",
                id, aggregate, envelope, createdAt);
        return id;
    }

    private String status(UUID id) {
        return jdbc.queryForObject("SELECT status FROM worker.outbox_relay_state WHERE event_id = ?",
                String.class, id);
    }

    @Test
    void anEventOfTheOutboxReachesTheBrokerOnceAndIsRecordedAsPublished() throws Exception {
        UUID id = insertHeld("2026-10-07T12:00:00Z");

        JobResult result = job.run();

        assertEquals(1, result.processed());
        GetResponse message = consumer.basicGet("relay.e2e", true);
        assertNotNull(message);
        assertEquals(id.toString(), message.getProps().getMessageId());
        assertEquals("corr-e2e", message.getProps().getHeaders().get("X-Correlation-Id").toString());
        assertEquals("PUBLISHED", status(id));
        assertNull(consumer.basicGet("relay.e2e", true));
        assertEquals(0, job.run().processed());
        assertNull(consumer.basicGet("relay.e2e", true));
    }

    @Test
    void theBookingOutboxIsLeftUntouched() {
        UUID id = insertHeld("2026-10-07T12:00:00Z");
        long before = jdbc.queryForObject("SELECT count(*) FROM booking.outbox_event", Long.class);

        job.run();

        assertEquals(before, jdbc.queryForObject("SELECT count(*) FROM booking.outbox_event", Long.class));
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM booking.outbox_event WHERE id = ?",
                Long.class, id));
    }

    @Test
    void anInvalidEnvelopeFailsForGoodWithoutReachingTheBroker() throws Exception {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO booking.outbox_event (id, aggregate_type, aggregate_id, event_type, payload, "
                + "created_at) VALUES (?, 'Reservation', gen_random_uuid(), 'ReservationHeld', '{}'::jsonb, now())", id);

        JobResult result = job.run();

        assertEquals(1, result.failed());
        assertEquals("FAILED_PERMANENT", status(id));
        assertNull(consumer.basicGet("relay.e2e", true));
        assertEquals(0, job.run().failed());
    }
}

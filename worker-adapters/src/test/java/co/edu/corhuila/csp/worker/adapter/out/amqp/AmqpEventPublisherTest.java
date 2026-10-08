package co.edu.corhuila.csp.worker.adapter.out.amqp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.application.port.out.OutboxEvent;
import co.edu.corhuila.csp.worker.application.port.out.PublishException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * The AMQP side of the relay: persistent, confirmed, mandatory publication with the event id as
 * message id. The tests that need a broker run when {@code TEST_AMQP_URL} is set, like the
 * database tests of the API with {@code TEST_DATABASE_URL}.
 */
class AmqpEventPublisherTest {

    private static final String EXCHANGE = "cine.events.test";
    private static final UUID ID = UUID.fromString("11111111-1111-4111-8111-111111111111");

    private static OutboxEvent event(String type, int version) {
        String envelope = "{\"eventId\":\"" + ID + "\",\"eventType\":\"" + type + "\",\"version\":" + version + "}";
        return new OutboxEvent(ID, type, UUID.randomUUID(), envelope, "corr-1", Instant.parse("2026-10-07T12:00:00Z"));
    }

    @Test
    void theRoutingKeyIsTheEventTypeInKebabCaseWithItsVersion() {
        assertEquals("cine.booking.reservation-held.v1",
                AmqpEventPublisher.routingKeyOf("ReservationHeld", 1));
        assertEquals("cine.booking.booking-confirmed.v1",
                AmqpEventPublisher.routingKeyOf("BookingConfirmed", 1));
        assertEquals("cine.booking.reservation-expired.v2",
                AmqpEventPublisher.routingKeyOf("ReservationExpired", 2));
    }

    @Test
    void aBrokerThatCannotBeReachedIsATransientFailure() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(URI.create("amqp://guest:guest@127.0.0.1:1"));
        factory.setConnectionTimeout(500);
        AmqpEventPublisher publisher = new AmqpEventPublisher(factory, EXCHANGE, Duration.ofSeconds(2));

        PublishException failure = assertThrows(PublishException.class,
                () -> publisher.publish(event("ReservationHeld", 1), "corr-1"));

        assertFalse(failure.isPermanent());
    }

    @Nested
    @EnabledIfEnvironmentVariable(named = "TEST_AMQP_URL", matches = ".+")
    class WithBroker {

        private ConnectionFactory factory;
        private Connection consumerConnection;
        private Channel consumer;
        private AmqpEventPublisher publisher;

        @BeforeEach
        void setUp() throws Exception {
            factory = new ConnectionFactory();
            factory.setUri(URI.create(System.getenv("TEST_AMQP_URL")));
            consumerConnection = factory.newConnection("test-consumer");
            consumer = consumerConnection.createChannel();
            consumer.exchangeDeclare(EXCHANGE, "topic", true);
            consumer.queueDeclare("relay.test", false, false, true, null);
            consumer.queuePurge("relay.test");
            consumer.queueBind("relay.test", EXCHANGE, "cine.booking.reservation-held.v1");
            consumer.queueBind("relay.test", EXCHANGE, "cine.booking.booking-confirmed.v1");
            publisher = new AmqpEventPublisher(factory, EXCHANGE, Duration.ofSeconds(5));
        }

        @AfterEach
        void tearDown() throws Exception {
            publisher.close();
            consumer.exchangeDelete(EXCHANGE);
            consumerConnection.close();
        }

        @Test
        void theEnvelopeIsPublishedPersistentWithTheEventIdAsMessageId() throws Exception {
            publisher.publish(event("ReservationHeld", 1), "corr-1");

            GetResponse message = consumer.basicGet("relay.test", true);
            assertNotNull(message);
            assertEquals("cine.booking.reservation-held.v1", message.getEnvelope().getRoutingKey());
            assertEquals(ID.toString(), message.getProps().getMessageId());
            assertEquals(2, message.getProps().getDeliveryMode());
            assertEquals("application/json", message.getProps().getContentType());
            assertEquals("ReservationHeld", message.getProps().getType());
            assertEquals("corr-1", message.getProps().getHeaders().get("X-Correlation-Id").toString());
            assertTrue(new String(message.getBody(), java.nio.charset.StandardCharsets.UTF_8)
                    .contains("\"eventType\":\"ReservationHeld\""));
        }

        @Test
        void aMessageNoQueueIsBoundToIsAPermanentFailure() {
            PublishException failure = assertThrows(PublishException.class,
                    () -> publisher.publish(event("ReservationExpired", 1), "corr-1"));

            assertTrue(failure.isPermanent());
        }

        @Test
        void thePublisherReconnectsAfterItsConnectionIsClosed() throws Exception {
            publisher.publish(event("ReservationHeld", 1), "corr-1");
            publisher.close();

            publisher.publish(event("BookingConfirmed", 1), "corr-2");

            assertNotNull(consumer.basicGet("relay.test", true));
            assertNotNull(consumer.basicGet("relay.test", true));
        }
    }
}

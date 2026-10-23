package co.edu.corhuila.csp.worker.adapter.out.amqp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.application.port.out.ReservationEvent;
import co.edu.corhuila.csp.worker.application.port.out.ReservationEvent.Type;
import co.edu.corhuila.csp.worker.application.port.out.ReservationEventQueue.Delivery;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * The queues of Concessions (ADR-036, ADR-045): the worker declares them bound to the events exchange with a
 * dead-letter queue each, takes one message at a time and settles it as the job asks. The tests that need a
 * broker run when {@code TEST_AMQP_URL} is set, like those of the publisher.
 */
class AmqpReservationEventsTest {

    static final String EXCHANGE = "cine.events.test";
    static final String DEAD_LETTER_EXCHANGE = "cine.dlx.test";
    static final String CONFIRMED_QUEUE = "cine.concessions.booking-confirmed";
    static final String EXPIRED_QUEUE = "cine.concessions.reservation-expired";
    private static final String RESERVATION = "44444444-4444-4444-8444-444444444444";

    static String envelope(String type, String reservationId) {
        return "{\"eventId\":\"" + UUID.randomUUID() + "\",\"eventType\":\"" + type + "\",\"version\":1,"
                + "\"aggregateId\":\"" + reservationId + "\",\"payload\":{\"reservationId\":\"" + reservationId + "\"}}";
    }

    @Test
    void aBrokerThatCannotBeReachedIsAFailureOfTheRunNotOfTheMessage() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(URI.create("amqp://guest:guest@127.0.0.1:1"));
        factory.setConnectionTimeout(500);

        assertThrows(IllegalStateException.class,
                () -> new AmqpReservationEvents(factory, EXCHANGE, DEAD_LETTER_EXCHANGE).next());
    }

    @Nested
    @EnabledIfEnvironmentVariable(named = "TEST_AMQP_URL", matches = ".+")
    class WithBroker {

        private ConnectionFactory factory;
        private Connection connection;
        private Channel channel;
        private AmqpReservationEvents queues;

        @BeforeEach
        void connect() throws Exception {
            factory = new ConnectionFactory();
            factory.setUri(URI.create(System.getenv("TEST_AMQP_URL")));
            connection = factory.newConnection("reservation-events-test");
            channel = connection.createChannel();
            channel.confirmSelect();
            channel.exchangeDeclare(EXCHANGE, "topic", true);
            channel.exchangeDeclare(DEAD_LETTER_EXCHANGE, "topic", true);
            queues = new AmqpReservationEvents(factory, EXCHANGE, DEAD_LETTER_EXCHANGE);
        }

        @AfterEach
        void disconnect() throws Exception {
            queues.close();
            for (String queue : new String[] {CONFIRMED_QUEUE, EXPIRED_QUEUE}) {
                channel.queueDelete(queue);
                channel.queueDelete(queue + ".dlq");
            }
            channel.exchangeDelete(EXCHANGE);
            channel.exchangeDelete(DEAD_LETTER_EXCHANGE);
            connection.close();
        }

        private void publish(String routingKey, String body) throws Exception {
            channel.basicPublish(EXCHANGE, routingKey, true,
                    new AMQP.BasicProperties.Builder().contentType("application/json").deliveryMode(2).build(),
                    body.getBytes(StandardCharsets.UTF_8));
            channel.waitForConfirmsOrDie(2_000);
        }

        private int depth(String queue) throws Exception {
            return channel.queueDeclarePassive(queue).getMessageCount();
        }

        @Test
        void anEmptyQueueGivesNothingAndTheQueuesAndTheirDeadLetterQueuesExist() throws Exception {
            assertEquals(Optional.empty(), queues.next());

            for (String queue : new String[] {CONFIRMED_QUEUE, EXPIRED_QUEUE}) {
                assertEquals(0, depth(queue));
                assertEquals(0, depth(queue + ".dlq"));
            }
        }

        @Test
        void aBookingConfirmedIsReadAsTheEventOfItsReservationAndLeavesTheQueueWhenAcknowledged() throws Exception {
            queues.next();
            String body = envelope("BookingConfirmed", RESERVATION);
            publish("cine.booking.booking-confirmed.v1", body);

            Delivery delivery = queues.next().orElseThrow();
            ReservationEvent event = delivery.event().orElseThrow();

            assertEquals(Type.BOOKING_CONFIRMED, event.type());
            assertEquals(RESERVATION, event.reservationId());
            assertTrue(body.contains(event.eventId()));
            delivery.ack();
            Thread.sleep(100);
            assertEquals(0, depth(CONFIRMED_QUEUE));
            assertEquals(Optional.empty(), queues.next());
        }

        @Test
        void aReservationExpiredIsReadFromItsOwnQueue() throws Exception {
            queues.next();
            publish("cine.booking.reservation-expired.v1", envelope("ReservationExpired", RESERVATION));

            ReservationEvent event = queues.next().orElseThrow().event().orElseThrow();

            assertEquals(Type.RESERVATION_EXPIRED, event.type());
            assertEquals(RESERVATION, event.reservationId());
        }

        @Test
        void aMessageThatIsRequeuedComesBackAndOneThatIsRejectedGoesToTheDeadLetterQueue() throws Exception {
            queues.next();
            publish("cine.booking.booking-confirmed.v1", envelope("BookingConfirmed", RESERVATION));

            queues.next().orElseThrow().requeue();
            Delivery again = queues.next().orElseThrow();
            assertTrue(again.event().isPresent());
            again.reject();
            Thread.sleep(200);

            assertEquals(0, depth(CONFIRMED_QUEUE));
            assertEquals(1, depth(CONFIRMED_QUEUE + ".dlq"));
            GetResponse dead = channel.basicGet(CONFIRMED_QUEUE + ".dlq", true);
            assertEquals("cine.booking.booking-confirmed.v1", dead.getEnvelope().getRoutingKey());
        }

        @Test
        void aMessageThatIsNotAReservationEventHasNoEvent() throws Exception {
            queues.next();
            publish("cine.booking.booking-confirmed.v1", "not json");
            publish("cine.booking.booking-confirmed.v1", envelope("BookingConfirmed", "not-a-uuid"));
            publish("cine.booking.booking-confirmed.v1", envelope("ReservationHeld", RESERVATION));

            for (int i = 0; i < 3; i++) {
                Delivery delivery = queues.next().orElseThrow();
                assertFalse(delivery.event().isPresent());
                delivery.reject();
            }
        }

        @Test
        void aMessageThatWasNotAcknowledgedWhenTheConnectionClosesIsDeliveredAgain() throws Exception {
            queues.next();
            publish("cine.booking.booking-confirmed.v1", envelope("BookingConfirmed", RESERVATION));
            queues.next().orElseThrow();

            queues.close();
            Thread.sleep(200);

            assertEquals(1, depth(CONFIRMED_QUEUE));
            assertTrue(new AmqpReservationEvents(factory, EXCHANGE, DEAD_LETTER_EXCHANGE).next().isPresent());
        }
    }
}

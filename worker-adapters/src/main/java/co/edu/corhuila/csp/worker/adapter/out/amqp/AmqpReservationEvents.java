package co.edu.corhuila.csp.worker.adapter.out.amqp;

import co.edu.corhuila.csp.worker.application.port.out.ReservationEvent;
import co.edu.corhuila.csp.worker.application.port.out.ReservationEvent.Type;
import co.edu.corhuila.csp.worker.application.port.out.ReservationEventQueue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * The queues where Concessions waits for the Booking events (ADR-036, ADR-045), read one message at a time.
 * Each consumer declares its own queues and bindings (ADR-036): here {@code cine.concessions.booking-confirmed}
 * and {@code cine.concessions.reservation-expired}, bound to the events exchange by the routing keys of Booking,
 * each with its dead-letter queue bound to the dead-letter exchange (ADR-007). The exchanges belong to
 * {@code csp-infra-rabbitmq} and are only checked, never declared: the login of Concessions may configure
 * {@code cine.concessions.*} and nothing else.
 *
 * <p>A message stays unacknowledged until the job settles it; if the connection is lost first, the broker gives
 * it back to the queue. A lost connection is a failure of the run: it is dropped and opened again on the next call.
 */
public class AmqpReservationEvents implements ReservationEventQueue, AutoCloseable {

    private record Source(String queue, String routingKey, String eventType, Type type) {
    }

    private static final List<Source> SOURCES = List.of(
            new Source("cine.concessions.booking-confirmed", "cine.booking.booking-confirmed.v1", "BookingConfirmed",
                    Type.BOOKING_CONFIRMED),
            new Source("cine.concessions.reservation-expired", "cine.booking.reservation-expired.v1",
                    "ReservationExpired", Type.RESERVATION_EXPIRED));

    private final ObjectMapper mapper = new ObjectMapper();
    private final ConnectionFactory factory;
    private final String exchange;
    private final String deadLetterExchange;
    private Connection connection;
    private Channel channel;
    private int first;

    public AmqpReservationEvents(ConnectionFactory factory, String exchange, String deadLetterExchange) {
        this.factory = factory;
        this.exchange = exchange;
        this.deadLetterExchange = deadLetterExchange;
    }

    @Override
    public synchronized Optional<Delivery> next() {
        try {
            Channel current = channel();
            // Each call starts at the other queue, so a long queue cannot keep the other one waiting.
            for (int i = 0; i < SOURCES.size(); i++) {
                Source source = SOURCES.get((first + i) % SOURCES.size());
                GetResponse response = current.basicGet(source.queue(), false);
                if (response != null) {
                    first = (first + i + 1) % SOURCES.size();
                    return Optional.of(new QueuedMessage(current, response, source));
                }
            }
            first = (first + 1) % SOURCES.size();
            return Optional.empty();
        } catch (IOException | TimeoutException | RuntimeException exception) {
            disconnect();
            throw new IllegalStateException("the broker could not be read", exception);
        }
    }

    @Override
    @PreDestroy
    public synchronized void close() {
        disconnect();
    }

    private Channel channel() throws IOException, TimeoutException {
        if (channel != null && channel.isOpen()) {
            return channel;
        }
        disconnect();
        connection = factory.newConnection("csp-worker-concessions-reservation-events");
        channel = connection.createChannel();
        channel.exchangeDeclarePassive(exchange);
        channel.exchangeDeclarePassive(deadLetterExchange);
        for (Source source : SOURCES) {
            String deadLetterQueue = source.queue() + ".dlq";
            channel.queueDeclare(deadLetterQueue, true, false, false, null);
            channel.queueBind(deadLetterQueue, deadLetterExchange, source.routingKey());
            channel.queueDeclare(source.queue(), true, false, false, Map.of("x-dead-letter-exchange", deadLetterExchange));
            channel.queueBind(source.queue(), exchange, source.routingKey());
        }
        return channel;
    }

    private void disconnect() {
        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (IOException | RuntimeException exception) {
            // the connection is already unusable: the next call opens a new one
        } finally {
            connection = null;
            channel = null;
        }
    }

    private final class QueuedMessage implements Delivery {

        private final Channel owner;
        private final long tag;
        private final Optional<ReservationEvent> event;

        QueuedMessage(Channel owner, GetResponse response, Source source) {
            this.owner = owner;
            this.tag = response.getEnvelope().getDeliveryTag();
            this.event = parse(new String(response.getBody(), StandardCharsets.UTF_8), source);
        }

        @Override
        public Optional<ReservationEvent> event() {
            return event;
        }

        @Override
        public void ack() {
            settle(() -> owner.basicAck(tag, false));
        }

        @Override
        public void reject() {
            settle(() -> owner.basicNack(tag, false, false));
        }

        @Override
        public void requeue() {
            settle(() -> owner.basicNack(tag, false, true));
        }

        private void settle(BrokerCall call) {
            try {
                call.run();
            } catch (IOException | RuntimeException exception) {
                throw new IllegalStateException("the broker did not take the answer for the message", exception);
            }
        }
    }

    @FunctionalInterface
    private interface BrokerCall {
        void run() throws IOException;
    }

    /** The event of the message when it is the expected one with a reservation id; empty otherwise. */
    private Optional<ReservationEvent> parse(String body, Source source) {
        try {
            JsonNode root = mapper.readTree(body);
            if (root == null || !source.eventType().equals(root.path("eventType").asText())) {
                return Optional.empty();
            }
            String reservationId = root.path("payload").path("reservationId").asText("");
            UUID.fromString(reservationId);
            return Optional.of(new ReservationEvent(root.path("eventId").asText(""), source.type(), reservationId));
        } catch (IOException | IllegalArgumentException exception) {
            return Optional.empty();
        }
    }
}

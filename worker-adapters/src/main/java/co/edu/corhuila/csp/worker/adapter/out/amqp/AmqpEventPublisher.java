package co.edu.corhuila.csp.worker.adapter.out.amqp;

import co.edu.corhuila.csp.worker.application.port.out.EventPublisher;
import co.edu.corhuila.csp.worker.application.port.out.OutboxEvent;
import co.edu.corhuila.csp.worker.application.port.out.PublishException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Publishes the booking events to RabbitMQ the way ADR-014 asks: persistent delivery, publisher
 * confirms, the {@code mandatory} flag and the event id as {@code messageId}, so a consumer
 * deduplicates the duplicates of an at-least-once delivery. The envelope goes out as it was
 * written; the relay never builds a new one.
 *
 * <p>A confirm that does not arrive, a nack or a lost connection is transient: the connection is
 * dropped and opened again on the next call. A message returned as unroutable is permanent: no
 * queue is bound to its routing key, and publishing it again cannot change that.
 */
public class AmqpEventPublisher implements EventPublisher, AutoCloseable {

    private static final String CORRELATION_HEADER = "X-Correlation-Id";

    private final ObjectMapper mapper = new ObjectMapper();
    private final ConnectionFactory factory;
    private final String exchange;
    private final long confirmTimeoutMillis;
    private final AtomicReference<String> returned = new AtomicReference<>();
    private Connection connection;
    private Channel channel;

    public AmqpEventPublisher(ConnectionFactory factory, String exchange, Duration confirmTimeout) {
        this.factory = factory;
        this.exchange = exchange;
        this.confirmTimeoutMillis = confirmTimeout.toMillis();
    }

    /** {@code cine.booking.<event-type-in-kebab-case>.v<version>}, as {@code events.md} lists them. */
    static String routingKeyOf(String eventType, int version) {
        String kebab = eventType.replaceAll("([a-z0-9])([A-Z])", "$1-$2").toLowerCase(Locale.ROOT);
        return "cine.booking." + kebab + ".v" + version;
    }

    @Override
    public synchronized void publish(OutboxEvent event, String correlationId) {
        String routingKey = routingKeyOf(event.eventType(), versionOf(event));
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .deliveryMode(2)
                .messageId(event.id().toString())
                .type(event.eventType())
                .timestamp(Date.from(event.createdAt()))
                .headers(Map.of(CORRELATION_HEADER, correlationId))
                .build();
        returned.set(null);
        try {
            Channel current = channel();
            current.basicPublish(exchange, routingKey, true, properties,
                    event.envelope().getBytes(StandardCharsets.UTF_8));
            current.waitForConfirmsOrDie(confirmTimeoutMillis);
        } catch (IOException | TimeoutException | InterruptedException | RuntimeException exception) {
            if (exception instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            disconnect();
            throw PublishException.transientFailure("the broker did not confirm the publication", exception);
        }
        if (returned.get() != null) {
            throw PublishException.permanent("the message is unroutable: " + returned.get(), null);
        }
    }

    @Override
    @PreDestroy
    public synchronized void close() {
        disconnect();
    }

    private int versionOf(OutboxEvent event) {
        try {
            JsonNode version = mapper.readTree(event.envelope()).path("version");
            if (version.isInt() && version.asInt() >= 1) {
                return version.asInt();
            }
        } catch (IOException exception) {
            throw PublishException.permanent("the envelope has no readable version", exception);
        }
        throw PublishException.permanent("the envelope has no readable version", null);
    }

    private Channel channel() throws IOException, TimeoutException {
        if (channel != null && channel.isOpen()) {
            return channel;
        }
        disconnect();
        connection = factory.newConnection("csp-worker-outbox-relay");
        channel = connection.createChannel();
        channel.confirmSelect();
        channel.exchangeDeclare(exchange, "topic", true);
        channel.addReturnListener(message -> returned.set(message.getRoutingKey()));
        return channel;
    }

    private void disconnect() {
        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (IOException | RuntimeException exception) {
            // the connection is already unusable: the next publication opens a new one
        } finally {
            connection = null;
            channel = null;
        }
    }
}

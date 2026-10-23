package co.edu.corhuila.csp.worker.adapter.out.amqp;

import co.edu.corhuila.csp.worker.adapter.out.outbox.RelayDomain;
import co.edu.corhuila.csp.worker.application.port.out.EventPublisher;
import co.edu.corhuila.csp.worker.application.port.out.OutboxEvent;
import co.edu.corhuila.csp.worker.application.port.out.PublishException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes the events of one domain to RabbitMQ the way ADR-014 asks: persistent delivery, publisher
 * confirms, the {@code mandatory} flag and the event id as {@code messageId}, so a consumer
 * deduplicates the duplicates of an at-least-once delivery. The envelope goes out as it was
 * written; the relay never builds a new one.
 *
 * <p>A confirm that does not arrive, a nack or a lost connection is transient: the connection is
 * dropped and opened again on the next call. The exchange belongs to {@code csp-infra-rabbitmq} (ADR-036) and the
 * login of the relay cannot declare anything, so the publisher only checks that it exists (a passive
 * declaration); one that does not exist is a transient failure too, because it is a configuration the
 * operator fixes and the relay recovers from by itself. A message returned as unroutable is permanent: no
 * queue is bound to its routing key, and publishing it again cannot change that.
 */
public class AmqpEventPublisher implements EventPublisher, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AmqpEventPublisher.class);
    private static final int NOT_FOUND = 404;
    private static final String CORRELATION_HEADER = "X-Correlation-Id";

    private final ObjectMapper mapper = new ObjectMapper();
    private final ConnectionFactory factory;
    private final String exchange;
    private final RelayDomain domain;
    private final long confirmTimeoutMillis;
    private final AtomicReference<String> returned = new AtomicReference<>();
    private Connection connection;
    private Channel channel;

    public AmqpEventPublisher(ConnectionFactory factory, String exchange, Duration confirmTimeout,
            RelayDomain domain) {
        if (factory.getVirtualHost().isEmpty()) {
            throw new IllegalArgumentException("the AMQP URL has an empty virtual host; the default one is written "
                    + "with its slash encoded, as in amqp://user:password@host:5672/%2F");
        }
        this.factory = factory;
        this.exchange = exchange;
        this.domain = domain;
        this.confirmTimeoutMillis = confirmTimeout.toMillis();
    }

    /** {@code cine.<domain>.<event-type-in-kebab-case>.v<version>}, as {@code events.md} lists them. */
    static String routingKeyOf(RelayDomain domain, String eventType, int version) {
        String kebab = eventType.replaceAll("([a-z0-9])([A-Z])", "$1-$2").toLowerCase(Locale.ROOT);
        return "cine." + domain.schema() + "." + kebab + ".v" + version;
    }

    @Override
    public synchronized void publish(OutboxEvent event, String correlationId) {
        String routingKey = routingKeyOf(domain, event.eventType(), versionOf(event));
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
        } catch (PublishException exception) {
            throw exception;
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
        connection = factory.newConnection("csp-worker-" + domain.schema() + "-outbox-relay");
        channel = connection.createChannel();
        channel.confirmSelect();
        verifyExchange();
        channel.addReturnListener(message -> returned.set(message.getRoutingKey()));
        return channel;
    }

    /** The relay may not declare the exchange: it checks it, which needs no permission of the login. */
    private void verifyExchange() {
        try {
            channel.exchangeDeclarePassive(exchange);
        } catch (IOException exception) {
            disconnect();
            if (isNotFound(exception)) {
                log.error("the exchange {} does not exist on the broker: create it with csp-infra-rabbitmq (ADR-036); "
                        + "the relay retries on its next run", exchange);
                throw PublishException.transientFailure("the exchange '" + exchange + "' does not exist on the broker",
                        exception);
            }
            throw PublishException.transientFailure("the exchange '" + exchange + "' could not be checked", exception);
        }
    }

    private static boolean isNotFound(IOException exception) {
        return exception.getCause() instanceof ShutdownSignalException signal
                && signal.getReason() instanceof AMQP.Channel.Close close && close.getReplyCode() == NOT_FOUND;
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

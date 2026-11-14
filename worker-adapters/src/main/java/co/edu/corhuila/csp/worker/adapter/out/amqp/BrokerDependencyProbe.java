package co.edu.corhuila.csp.worker.adapter.out.amqp;

import co.edu.corhuila.csp.worker.application.port.out.DependencyProbe;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Asks RabbitMQ whether it answers by opening a connection of its own with the login of the relay and closing it,
 * so the check never touches the connection that publishes. It declares nothing: the login cannot.
 */
public class BrokerDependencyProbe implements DependencyProbe {

    private static final Logger log = LoggerFactory.getLogger(BrokerDependencyProbe.class);

    private final ConnectionFactory factory;

    public BrokerDependencyProbe(ConnectionFactory factory) {
        this.factory = factory;
    }

    @Override
    public String dependency() {
        return "rabbitmq";
    }

    @Override
    public boolean isReachable() {
        try (Connection connection = factory.newConnection()) {
            return connection.isOpen();
        } catch (IOException | TimeoutException | RuntimeException unreachable) {
            log.warn("readiness: rabbitmq does not answer: {}", unreachable.getClass().getSimpleName());
            return false;
        }
    }
}

package co.edu.corhuila.csp.worker.adapter.out.amqp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.rabbitmq.client.ConnectionFactory;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

class BrokerDependencyProbeTest {

    @Test
    void itIsNamedRabbitmq() {
        assertEquals("rabbitmq", new BrokerDependencyProbe(new ConnectionFactory()).dependency());
    }

    @Test
    void aClosedPortIsNotReachableAndTheAnswerIsQuick() throws Exception {
        int closed;
        try (ServerSocket socket = new ServerSocket(0)) {
            closed = socket.getLocalPort();
        }
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(URI.create("amqp://user:password@localhost:" + closed + "/%2F"));
        factory.setConnectionTimeout(2_000);
        BrokerDependencyProbe probe = new BrokerDependencyProbe(factory);

        assertTimeoutPreemptively(Duration.ofSeconds(8), () -> assertFalse(probe.isReachable()));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "TEST_AMQP_URL", matches = ".+")
    void aRunningBrokerIsReachable() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(URI.create(System.getenv("TEST_AMQP_URL")));
        factory.setConnectionTimeout(5_000);

        assertTrue(new BrokerDependencyProbe(factory).isReachable());
    }
}

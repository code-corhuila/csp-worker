package co.edu.corhuila.csp.worker.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.adapter.in.http.HealthController;
import co.edu.corhuila.csp.worker.application.usecase.ReadinessCheck;
import com.zaxxer.hikari.HikariDataSource;
import java.net.ServerSocket;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * The readiness asks the dependencies of the relays that were composed, and a worker with none depends on nothing.
 * The health controller is found by the scan of the application, so it is added here as the application does.
 */
class ReadinessWiringTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(ReadinessConfiguration.class, HealthController.class);

    private static int closedPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    void aWorkerWithNoRelayIsReadyAndTheHealthPointIsComposed() {
        runner.run(context -> {
            assertTrue(context.getBean(ReadinessCheck.class).check().ready());
            assertTrue(context.getBean(ReadinessCheck.class).check().dependencies().isEmpty());
            assertEquals(1, context.getBeansOfType(HealthController.class).size());
        });
    }

    @Test
    void aRelayDatabaseThatDoesNotAnswerKeepsTheWorkerNotReady() throws Exception {
        int closed = closedPort();
        runner.withBean("relayDataSource", HikariDataSource.class, () -> {
            HikariDataSource dataSource = new HikariDataSource();
            dataSource.setJdbcUrl("jdbc:postgresql://localhost:" + closed + "/csp");
            dataSource.setUsername("worker_app");
            dataSource.setPassword("x");
            return dataSource;
        }).run(context -> {
            ReadinessCheck.Report report = context.getBean(ReadinessCheck.class).check();

            assertFalse(report.ready());
            assertEquals(Map.of("postgresql", false), report.dependencies());
        });
    }
}

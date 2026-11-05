package co.edu.corhuila.csp.worker.adapter.out.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

class JdbcDependencyProbeTest {

    @Test
    void itIsNamedPostgresql() {
        assertEquals("postgresql", new JdbcDependencyProbe("jdbc:postgresql://localhost:1/csp", "u", "p").dependency());
    }

    @Test
    void aClosedPortIsNotReachableAndTheAnswerIsQuick() throws Exception {
        int closed;
        try (ServerSocket socket = new ServerSocket(0)) {
            closed = socket.getLocalPort();
        }
        JdbcDependencyProbe probe = new JdbcDependencyProbe("jdbc:postgresql://localhost:" + closed + "/csp", "u", "p");

        assertTimeoutPreemptively(Duration.ofSeconds(8), () -> assertFalse(probe.isReachable()));
    }

    @Test
    void aMalformedUrlIsNotReachableAndDoesNotThrow() {
        assertFalse(new JdbcDependencyProbe("not a url", "u", "p").isReachable());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "TEST_DATABASE_URL", matches = ".+")
    void aRunningInstanceIsReachable() throws Exception {
        String url = System.getenv("TEST_DATABASE_URL");
        String base = url.contains("?") ? url.substring(0, url.indexOf('?')) : url;
        String query = url.contains("?") ? url.substring(url.indexOf('?') + 1) : "";
        String user = valueOf(query, "user");
        String password = valueOf(query, "password");

        assertTrue(new JdbcDependencyProbe(base, user, password).isReachable());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "TEST_DATABASE_URL", matches = ".+")
    void aWrongPasswordIsNotReachable() {
        String url = System.getenv("TEST_DATABASE_URL");
        String base = url.contains("?") ? url.substring(0, url.indexOf('?')) : url;
        String query = url.contains("?") ? url.substring(url.indexOf('?') + 1) : "";

        assertFalse(new JdbcDependencyProbe(base, valueOf(query, "user"), "not-the-password").isReachable());
    }

    private static String valueOf(String query, String key) {
        for (String pair : query.split("&")) {
            if (pair.startsWith(key + "=")) {
                return pair.substring(key.length() + 1);
            }
        }
        return "";
    }
}

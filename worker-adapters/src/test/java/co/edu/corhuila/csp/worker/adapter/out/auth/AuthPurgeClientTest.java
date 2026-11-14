package co.edu.corhuila.csp.worker.adapter.out.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.application.port.out.PurgeResult;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The client against a stand-in for the identity service that answers as its contract does: the
 * route lives under the context path of the service, and the purge is authenticated and traced.
 */
class AuthPurgeClientTest {

    private static final String CONTEXT = "/api/v1/auth";

    private HttpServer server;
    private final List<String> paths = new CopyOnWriteArrayList<>();
    private final List<String> authorizations = new CopyOnWriteArrayList<>();
    private final List<String> correlations = new CopyOnWriteArrayList<>();
    private final List<String> contentTypes = new CopyOnWriteArrayList<>();
    private final List<String> bodies = new CopyOnWriteArrayList<>();
    private final List<Integer> answers = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startTheService() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            paths.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            correlations.add(exchange.getRequestHeaders().getFirst("X-Correlation-Id"));
            contentTypes.add(exchange.getRequestHeaders().getFirst("Content-Type"));
            bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            int status = answers.isEmpty() ? 200 : answers.remove(0);
            byte[] body = (status == 200 ? "{\"processedCount\":37,\"hasMore\":true}" : "{\"error\":\"X\"}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopTheService() {
        server.stop(0);
    }

    @Test
    void thePurgeCallsTheRouteBehindTheContextPathWithItsTokenItsCorrelationIdAndTheBatchSize() {
        PurgeResult result = client().purge("run-1", 250);

        assertEquals(new PurgeResult(37, true), result);
        assertEquals(List.of("POST " + CONTEXT + "/internal/maintenance/purge"), paths);
        assertEquals(List.of("Bearer service-token"), authorizations);
        assertEquals(List.of("run-1"), correlations);
        assertEquals(List.of("application/json"), contentTypes);
        assertEquals(List.of("{\"batchSize\":250}"), bodies);
    }

    @Test
    void aServerErrorIsRetriedUntilItRecovers() {
        answers.addAll(List.of(503, 429));

        assertEquals(37, client().purge("run-2", 10).processedCount());
        assertEquals(3, paths.size());
    }

    @Test
    void aClientErrorIsNeverRetried() {
        for (int status : new int[] {400, 401, 403}) {
            paths.clear();
            answers.add(status);

            assertThrows(IllegalStateException.class, () -> client().purge("run-3", 10));
            assertEquals(1, paths.size());
        }
    }

    @Test
    void theRetriesAreBounded() {
        answers.addAll(List.of(503, 503, 503, 503));

        assertThrows(IllegalStateException.class, () -> client().purge("run-4", 10));
        assertEquals(3, paths.size());
    }

    @Test
    void anUnreachableServiceFailsAfterTheBoundedRetries() {
        server.stop(0);

        assertThrows(IllegalStateException.class, () -> client().purge("run-5", 10));
    }

    private AuthPurgeClient client() {
        return new AuthPurgeClient("http://127.0.0.1:" + server.getAddress().getPort() + CONTEXT + "/",
                new ServiceTokenSource("service-token", ""), 5);
    }
}

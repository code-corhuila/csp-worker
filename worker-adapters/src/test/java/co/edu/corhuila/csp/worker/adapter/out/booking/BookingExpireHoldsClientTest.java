package co.edu.corhuila.csp.worker.adapter.out.booking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import co.edu.corhuila.csp.worker.application.port.out.ExpireHoldsResult;
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
 * The client against a stand-in for the booking service that answers as its contract does: the
 * route lives under the context path of the service, and the sweep is authenticated and traced.
 */
class BookingExpireHoldsClientTest {

    private static final String CONTEXT = "/api/v1/booking";

    private HttpServer server;
    private final List<String> paths = new CopyOnWriteArrayList<>();
    private final List<String> authorizations = new CopyOnWriteArrayList<>();
    private final List<String> correlations = new CopyOnWriteArrayList<>();
    private final List<Integer> answers = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startTheService() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            paths.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            correlations.add(exchange.getRequestHeaders().getFirst("X-Correlation-Id"));
            int status = answers.isEmpty() ? 200 : answers.remove(0);
            byte[] body = (status == 200 ? "{\"expired\":4,\"remaining\":1}" : "{\"error\":\"X\"}")
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
    void theSweepCallsTheRouteBehindTheContextPathWithItsTokenAndItsCorrelationId() {
        ExpireHoldsResult result = client().expireHolds("run-1");

        assertEquals(new ExpireHoldsResult(4, 1), result);
        assertEquals(List.of("POST " + CONTEXT + "/internal/maintenance/expire-holds"), paths);
        assertEquals(List.of("Bearer service-token"), authorizations);
        assertEquals(List.of("run-1"), correlations);
    }

    @Test
    void aServerErrorIsRetriedUntilItRecovers() {
        answers.addAll(List.of(503, 429));

        assertEquals(4, client().expireHolds("run-2").expired());
        assertEquals(3, paths.size());
    }

    @Test
    void aClientErrorIsNeverRetried() {
        answers.add(403);

        assertThrows(IllegalStateException.class, () -> client().expireHolds("run-3"));
        assertEquals(1, paths.size());
    }

    @Test
    void theRetriesAreBounded() {
        answers.addAll(List.of(503, 503, 503, 503));

        assertThrows(IllegalStateException.class, () -> client().expireHolds("run-4"));
        assertEquals(3, paths.size());
    }

    private BookingExpireHoldsClient client() {
        return new BookingExpireHoldsClient("http://127.0.0.1:" + server.getAddress().getPort() + CONTEXT + "/",
                "service-token", 5);
    }
}

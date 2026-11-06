package co.edu.corhuila.csp.worker.adapter.out.ticketing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.application.port.out.TicketingApiException;
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
 * The client against a stand-in for csp-ticketing-api that answers as its internal route does: the route sits at the
 * root of the service, takes the limit as a query parameter and answers {@code {"warned": n}}.
 */
class TicketingHoldExpiryWarningsClientTest {

    private static final String ROUTE = "POST /internal/notifications/hold-expiry-warnings?limit=100";

    private HttpServer server;
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final List<String> authorizations = new CopyOnWriteArrayList<>();
    private final List<String> correlations = new CopyOnWriteArrayList<>();
    private final List<Integer> statuses = new CopyOnWriteArrayList<>();
    private volatile String body = "{\"warned\":4}";
    private volatile long delayMillis;

    @BeforeEach
    void startTheService() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            correlations.add(exchange.getRequestHeaders().getFirst("X-Correlation-Id"));
            if (delayMillis > 0) {
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            int status = statuses.isEmpty() ? 200 : statuses.remove(0);
            byte[] answer = (status == 200 ? body : "{\"error\":\"X\"}").getBytes(StandardCharsets.UTF_8);
            try {
                exchange.sendResponseHeaders(status, answer.length);
                exchange.getResponseBody().write(answer);
            } catch (IOException ignored) {
                // the client gave up before the answer, which is what the timeout test wants
            }
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopTheService() {
        server.stop(0);
    }

    private TicketingHoldExpiryWarningsClient client(long timeoutSeconds) {
        return new TicketingHoldExpiryWarningsClient("http://127.0.0.1:" + server.getAddress().getPort() + "/",
                new ServiceTokenSource("service-token", ""), timeoutSeconds);
    }

    @Test
    void warnCallsItsRouteWithTheLimitTheTokenAndTheCorrelationIdAndReadsTheCount() {
        assertEquals(4, client(5).warn("run-1", 100));

        assertEquals(List.of(ROUTE), requests);
        assertEquals(List.of("Bearer service-token"), authorizations);
        assertEquals(List.of("run-1"), correlations);
    }

    @Test
    void aRefusalOfTheServiceIsAnErrorAndIsNotRetried() {
        statuses.add(403);

        assertThrows(TicketingApiException.class, () -> client(5).warn("run-2", 100));
        assertEquals(1, requests.size());
    }

    @Test
    void aFailingServiceIsRetriedAndTheLaterAnswerIsUsed() {
        statuses.add(503);

        assertEquals(4, client(5).warn("run-3", 100));
        assertEquals(2, requests.size());
    }

    @Test
    void anAnswerWithoutAValidCountIsAnError() {
        body = "{\"other\":1}";
        assertThrows(TicketingApiException.class, () -> client(5).warn("run-4", 100));

        body = "not json";
        assertThrows(TicketingApiException.class, () -> client(5).warn("run-5", 100));
    }

    @Test
    void aNotAnsweringServiceIsAnError() {
        TicketingHoldExpiryWarningsClient client = new TicketingHoldExpiryWarningsClient("http://127.0.0.1:1",
                new ServiceTokenSource("service-token", ""), 1);

        assertThrows(TicketingApiException.class, () -> client.warn("run-6", 100));
    }

    @Test
    void aServiceThatAnswersAfterTheLimitIsAnError() {
        delayMillis = 3000;

        assertThrows(TicketingApiException.class, () -> client(1).warn("run-7", 100));
    }
}

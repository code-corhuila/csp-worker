package co.edu.corhuila.csp.worker.adapter.out.ticketing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.application.port.out.TicketingApiException;
import co.edu.corhuila.csp.worker.application.port.out.TicketingDispatchApi.Result;
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
 * root of the service, takes the limit as a query parameter and answers the three counts.
 */
class TicketingDispatchClientTest {

    private HttpServer server;
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final List<String> authorizations = new CopyOnWriteArrayList<>();
    private final List<String> correlations = new CopyOnWriteArrayList<>();
    private final List<Integer> answers = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startTheService() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            correlations.add(exchange.getRequestHeaders().getFirst("X-Correlation-Id"));
            int status = answers.isEmpty() ? 200 : answers.remove(0);
            byte[] body = (status == 200 ? "{\"sent\":4,\"retried\":2,\"failed\":1}" : "{\"error\":\"X\"}")
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

    private TicketingDispatchClient client() {
        return new TicketingDispatchClient("http://127.0.0.1:" + server.getAddress().getPort() + "/",
                new ServiceTokenSource("service-token", ""), 5);
    }

    @Test
    void dispatchCallsItsRouteWithTheLimitTheTokenAndTheCorrelationIdAndReadsTheCounts() {
        Result result = client().dispatch("run-1", 25);

        assertEquals(new Result(4, 2, 1), result);
        assertEquals(7, result.handled());
        assertEquals(List.of("POST /internal/fulfillments/dispatch?limit=25"), requests);
        assertEquals(List.of("Bearer service-token"), authorizations);
        assertEquals(List.of("run-1"), correlations);
    }

    @Test
    void aRefusalOfTheServiceIsAnError() {
        answers.add(400);

        assertThrows(TicketingApiException.class, () -> client().dispatch("run-2", 25));
    }

    @Test
    void aNotAnsweringServiceIsAnError() {
        TicketingDispatchClient client = new TicketingDispatchClient("http://127.0.0.1:1",
                new ServiceTokenSource("service-token", ""), 1);

        assertThrows(TicketingApiException.class, () -> client.dispatch("run-3", 25));
    }
}

package co.edu.corhuila.csp.worker.adapter.out.concessions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.application.port.out.ExpireOrdersResult;
import co.edu.corhuila.csp.worker.application.port.out.OrdersApiException;
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
 * The client against a stand-in for the Concessions service that answers as its contract does: the internal
 * routes live under the context path, answer {@code 204}, and take only the worker's token.
 */
class ConcessionsOrdersClientTest {

    private static final String CONTEXT = "/api/v1/concessions";
    private static final String RESERVATION = "44444444-4444-4444-8444-444444444444";

    private HttpServer server;
    private final List<String> paths = new CopyOnWriteArrayList<>();
    private final List<String> authorizations = new CopyOnWriteArrayList<>();
    private final List<String> correlations = new CopyOnWriteArrayList<>();
    private final List<Integer> answers = new CopyOnWriteArrayList<>();
    private final List<String> bodies = new CopyOnWriteArrayList<>();
    private final List<String> contentTypes = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startTheService() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            paths.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            correlations.add(exchange.getRequestHeaders().getFirst("X-Correlation-Id"));
            bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            contentTypes.add(String.valueOf(exchange.getRequestHeaders().getFirst("Content-Type")));
            int status = answers.isEmpty() ? 204 : answers.remove(0);
            if (status == 204) {
                exchange.sendResponseHeaders(204, -1);
            } else {
                byte[] body = (status == 200 ? "{\"processedCount\":37,\"hasMore\":true}" : "{\"error\":\"X\"}")
                        .getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status, body.length);
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopTheService() {
        server.stop(0);
    }

    @Test
    void confirmingCallsTheRouteBehindTheContextPathWithTheTokenAndTheCorrelationId() {
        client().confirmByReservation(RESERVATION, "run-1");

        assertEquals(List.of("POST " + CONTEXT + "/internal/orders/by-reservation/" + RESERVATION + "/confirm"), paths);
        assertEquals(List.of("Bearer service-token"), authorizations);
        assertEquals(List.of("run-1"), correlations);
    }

    @Test
    void releasingCallsItsOwnRoute() {
        client().releaseByReservation(RESERVATION, "run-2");

        assertEquals(List.of("POST " + CONTEXT + "/internal/orders/by-reservation/" + RESERVATION + "/release"), paths);
    }

    @Test
    void theSweepCallsItsRouteWithTheBatchSizeAndReadsWhatWasReleased() {
        answers.add(200);

        ExpireOrdersResult result = client().expireOrders("run-8", 250);

        assertEquals(new ExpireOrdersResult(37, true), result);
        assertEquals(List.of("POST " + CONTEXT + "/internal/orders/expire"), paths);
        assertEquals(List.of("Bearer service-token"), authorizations);
        assertEquals(List.of("run-8"), correlations);
        assertEquals(List.of("{\"batchSize\":250}"), bodies);
        assertEquals(List.of("application/json"), contentTypes);
    }

    @Test
    void aFailingSweepIsRetriedLikeAnyOtherCall() {
        answers.addAll(List.of(503, 200));

        assertEquals(37, client().expireOrders("run-9", 10).processedCount());
        assertEquals(2, paths.size());
    }

    @Test
    void aBusyOrFailingServiceIsRetriedUntilItRecovers() {
        answers.addAll(List.of(503, 429));

        client().confirmByReservation(RESERVATION, "run-3");

        assertEquals(3, paths.size());
    }

    @Test
    void theRetriesAreBoundedAndTheFailureCanBeRetriedLater() {
        answers.addAll(List.of(500, 500, 500, 500, 500));

        OrdersApiException failure = assertThrows(OrdersApiException.class,
                () -> client().confirmByReservation(RESERVATION, "run-4"));

        assertTrue(failure.retryable());
        assertEquals(3, paths.size());
    }

    @Test
    void aConflictOrAnUnknownRequestIsRefusedForGoodAndNeverRetried() {
        for (int status : new int[] {400, 404, 409}) {
            paths.clear();
            answers.add(status);

            OrdersApiException failure = assertThrows(OrdersApiException.class,
                    () -> client().confirmByReservation(RESERVATION, "run-5"));

            assertFalse(failure.retryable(), "status " + status);
            assertEquals(1, paths.size());
        }
    }

    @Test
    void aRefusedTokenIsTheWorkersProblemNotTheMessagesSoItCanBeRetriedLater() {
        for (int status : new int[] {401, 403}) {
            paths.clear();
            answers.add(status);

            OrdersApiException failure = assertThrows(OrdersApiException.class,
                    () -> client().releaseByReservation(RESERVATION, "run-6"));

            assertTrue(failure.retryable(), "status " + status);
            assertEquals(1, paths.size());
        }
    }

    @Test
    void anUnreachableServiceCanBeRetriedLater() {
        server.stop(0);

        OrdersApiException failure = assertThrows(OrdersApiException.class,
                () -> client().confirmByReservation(RESERVATION, "run-7"));

        assertTrue(failure.retryable());
    }

    private ConcessionsOrdersClient client() {
        String url = "http://127.0.0.1:" + server.getAddress().getPort() + CONTEXT;
        return new ConcessionsOrdersClient(url, new ServiceTokenSource("service-token", ""), 5);
    }
}

package co.edu.corhuila.csp.worker.adapter.out.concessions;

import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.adapter.out.http.RetryingHttp;
import co.edu.corhuila.csp.worker.application.port.out.ConcessionsOrdersApi;
import co.edu.corhuila.csp.worker.application.port.out.ExpireOrdersResult;
import co.edu.corhuila.csp.worker.application.port.out.OrdersApiException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

/**
 * HTTP client for the internal operations of the Concessions service that settle its orders (ADR-045): the
 * order of a reservation, and the sweep of the orders whose time ran out. The worker authenticates with its service token (Norma 5.7.4) and sends the correlation id of the
 * run on every call (Norma 5.7.3). It retries only what a retry can fix, as {@link RetryingHttp} does, and tells
 * the job whether a failure is worth trying again later: the network, a busy service and a refused token are;
 * a refusal of the message itself (400, 404 or a 409 between the order and the event) is not.
 */
@Component
@ConditionalOnExpression("${worker.concessions-reservation-events.enabled:false} or ${worker.expire-concessions-orders.enabled:false}")
public class ConcessionsOrdersClient implements ConcessionsOrdersApi {

    private static final String ROUTE = "/internal/orders/by-reservation/";
    private static final String EXPIRE_ROUTE = "/internal/orders/expire";

    private final RetryingHttp http;
    private final ObjectMapper json = new ObjectMapper();
    private final String baseUrl;
    private final ServiceTokenSource serviceToken;
    private final Duration timeout;

    /**
     * @param concessionsApiUrl the base of the Concessions service, with its context path
     *     ({@code /api/v1/concessions})
     */
    public ConcessionsOrdersClient(
            @Value("${concessions.api.url:http://concessions-api:8085/api/v1/concessions}") String concessionsApiUrl,
            ServiceTokenSource serviceToken,
            @Value("${concessions.api.timeout-seconds:10}") long timeoutSeconds) {
        this.baseUrl = concessionsApiUrl.replaceAll("/+$", "");
        this.serviceToken = serviceToken;
        this.timeout = Duration.ofSeconds(timeoutSeconds);
        this.http = new RetryingHttp(HttpClient.newBuilder().connectTimeout(timeout).build(), "concessions service");
    }

    @Override
    public void confirmByReservation(String reservationId, String correlationId) {
        post(reservationId, "confirm", correlationId);
    }

    @Override
    public void releaseByReservation(String reservationId, String correlationId) {
        post(reservationId, "release", correlationId);
    }

    @Override
    public ExpireOrdersResult expireOrders(String correlationId, int batchSize) {
        String answer = send(post(EXPIRE_ROUTE, correlationId)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body(batchSize))));
        try {
            JsonNode node = json.readTree(answer);
            return new ExpireOrdersResult(node.path("processedCount").asInt(0), node.path("hasMore").asBoolean(false));
        } catch (IOException exception) {
            throw new OrdersApiException("the answer of the concessions service is not valid JSON", false, exception);
        }
    }

    private void post(String reservationId, String action, String correlationId) {
        send(post(ROUTE + reservationId + "/" + action, correlationId).POST(HttpRequest.BodyPublishers.noBody()));
    }

    /** A request to a route of the service with the token of the worker, read now because it may be rotated. */
    private HttpRequest.Builder post(String route, String correlationId) {
        return HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + route))
                .header("Authorization", "Bearer " + serviceToken.current())
                .header("X-Correlation-Id", correlationId)
                .timeout(timeout);
    }

    private String send(HttpRequest.Builder request) {
        try {
            return http.send(request.build());
        } catch (RetryingHttp.Failure failure) {
            throw new OrdersApiException(failure.getMessage(), retryable(failure.status()), failure);
        }
    }

    private String body(int batchSize) {
        try {
            return json.writeValueAsString(Map.of("batchSize", batchSize));
        } catch (IOException exception) {
            throw new IllegalStateException("the sweep request cannot be written", exception);
        }
    }

    /** The network (0), a busy or failing service and a refused token can work later; the rest cannot. */
    private static boolean retryable(int status) {
        return status == 0 || status == 401 || status == 403 || status == 429 || status >= 500;
    }
}

package co.edu.corhuila.csp.worker.adapter.out.concessions;

import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.adapter.out.http.RetryingHttp;
import co.edu.corhuila.csp.worker.application.port.out.ConcessionsOrdersApi;
import co.edu.corhuila.csp.worker.application.port.out.OrdersApiException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * HTTP client for the internal operations of the Concessions service that settle the order of a reservation
 * (ADR-045). The worker authenticates with its service token (Norma 5.7.4) and sends the correlation id of the
 * run on every call (Norma 5.7.3). It retries only what a retry can fix, as {@link RetryingHttp} does, and tells
 * the job whether a failure is worth trying again later: the network, a busy service and a refused token are;
 * a refusal of the message itself (400, 404 or a 409 between the order and the event) is not.
 */
@Component
@ConditionalOnProperty(prefix = "worker.concessions-reservation-events", name = "enabled", havingValue = "true")
public class ConcessionsOrdersClient implements ConcessionsOrdersApi {

    private static final String ROUTE = "/internal/orders/by-reservation/";

    private final RetryingHttp http;
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

    private void post(String reservationId, String action, String correlationId) {
        // The token is read now, not at startup: it has a lifetime and the platform may rotate it.
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + ROUTE + reservationId + "/" + action))
                .header("Authorization", "Bearer " + serviceToken.current())
                .header("X-Correlation-Id", correlationId)
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        try {
            http.send(request);
        } catch (RetryingHttp.Failure failure) {
            throw new OrdersApiException(failure.getMessage(), retryable(failure.status()), failure);
        }
    }

    /** The network (0), a busy or failing service and a refused token can work later; the rest cannot. */
    private static boolean retryable(int status) {
        return status == 0 || status == 401 || status == 403 || status == 429 || status >= 500;
    }
}

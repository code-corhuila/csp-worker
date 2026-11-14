package co.edu.corhuila.csp.worker.adapter.out.ticketing;

import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.adapter.out.http.RetryingHttp;
import co.edu.corhuila.csp.worker.application.port.out.TicketingApiException;
import co.edu.corhuila.csp.worker.application.port.out.TicketingHoldExpiryWarningsApi;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * HTTP client for the internal operation of the Ticketing service that creates the warnings of the holds about to
 * expire (HU-TICKETING-005). The worker authenticates with its service token (Norma 5.7.4) and sends the correlation
 * id of the run (Norma 5.7.3). The internal routes of Ticketing sit at the root of the service. The connection and the
 * reading of the answer share the same limit, and a failure is retried only as {@link RetryingHttp} does.
 */
@Component
@ConditionalOnProperty(prefix = "worker.hold-expiry-warnings", name = "enabled", havingValue = "true")
public class TicketingHoldExpiryWarningsClient implements TicketingHoldExpiryWarningsApi {

    private static final String ROUTE = "/internal/notifications/hold-expiry-warnings";

    private final RetryingHttp http;
    private final ObjectMapper json = new ObjectMapper();
    private final String baseUrl;
    private final ServiceTokenSource serviceToken;
    private final Duration timeout;

    /** @param ticketingApiUrl the base of the Ticketing service, where its internal routes start */
    public TicketingHoldExpiryWarningsClient(
            @Value("${ticketing.api.url:http://ticketing-api:8084}") String ticketingApiUrl,
            ServiceTokenSource serviceToken,
            @Value("${ticketing.api.timeout-seconds:10}") long timeoutSeconds) {
        this.baseUrl = ticketingApiUrl.replaceAll("/+$", "");
        this.serviceToken = serviceToken;
        this.timeout = Duration.ofSeconds(timeoutSeconds);
        this.http = new RetryingHttp(HttpClient.newBuilder().connectTimeout(timeout).build(), "ticketing service");
    }

    @Override
    public int warn(String correlationId, int limit) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + ROUTE + "?limit=" + limit))
                .header("Authorization", "Bearer " + serviceToken.current())
                .header("X-Correlation-Id", correlationId)
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        String answer;
        try {
            answer = http.send(request);
        } catch (RetryingHttp.Failure failure) {
            throw new TicketingApiException(failure.getMessage(), failure);
        }
        JsonNode warned;
        try {
            warned = json.readTree(answer).path("warned");
        } catch (IOException exception) {
            throw new TicketingApiException("the answer of the ticketing service is not valid JSON", exception);
        }
        if (!warned.isInt() || warned.asInt() < 0) {
            throw new TicketingApiException("the answer of the ticketing service has no valid warned count");
        }
        return warned.asInt();
    }
}

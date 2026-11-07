package co.edu.corhuila.csp.worker.adapter.out.booking;

import co.edu.corhuila.csp.worker.adapter.out.http.RetryingHttp;
import co.edu.corhuila.csp.worker.application.port.out.BookingExpireHoldsApi;
import co.edu.corhuila.csp.worker.application.port.out.ExpireHoldsResult;
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
 * HTTP client for the internal maintenance operation of the booking service. The worker
 * authenticates with its service token (Norma 5.7.4) and sends its own correlation id on every
 * call (Norma 5.7.3). It retries only what a retry can fix, as {@link RetryingHttp} does.
 */
@Component
@ConditionalOnProperty(prefix = "worker.expire-holds", name = "enabled", havingValue = "true", matchIfMissing = true)
public class BookingExpireHoldsClient implements BookingExpireHoldsApi {

    private static final String OPERATION = "/internal/maintenance/expire-holds";

    private final RetryingHttp http;
    private final ObjectMapper json = new ObjectMapper();
    private final String operationUrl;
    private final ServiceTokenSource serviceToken;
    private final Duration timeout;

    /**
     * @param bookingApiUrl the base of the booking service, with its context path: the service
     *     answers under {@code /api/v1/booking}, so the operation is {@code <base>/internal/...}
     */
    public BookingExpireHoldsClient(
            @Value("${booking.api.url:http://booking-api:8083/api/v1/booking}") String bookingApiUrl,
            ServiceTokenSource serviceToken,
            @Value("${booking.api.timeout-seconds:10}") long timeoutSeconds) {
        this.operationUrl = bookingApiUrl.replaceAll("/+$", "") + OPERATION;
        this.serviceToken = serviceToken;
        this.timeout = Duration.ofSeconds(timeoutSeconds);
        this.http = new RetryingHttp(HttpClient.newBuilder().connectTimeout(timeout).build(), "booking service");
    }

    @Override
    public ExpireHoldsResult expireHolds(String correlationId) {
        // The token is read now, not at startup: it has a lifetime and the platform may rotate it.
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(operationUrl))
                .header("Authorization", "Bearer " + serviceToken.current())
                .header("X-Correlation-Id", correlationId)
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        return parse(http.send(request));
    }

    private ExpireHoldsResult parse(String body) {
        try {
            JsonNode node = json.readTree(body);
            return new ExpireHoldsResult(node.path("expired").asInt(0), node.path("remaining").asInt(0));
        } catch (IOException exception) {
            throw new IllegalStateException("the answer of the booking service is not valid JSON", exception);
        }
    }
}

package co.edu.corhuila.csp.worker.adapter.out.booking;

import co.edu.corhuila.csp.worker.application.port.out.BookingExpireHoldsApi;
import co.edu.corhuila.csp.worker.application.port.out.ExpireHoldsResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * HTTP client for the internal maintenance operation of the booking service. The worker
 * authenticates with its service token (Norma 5.7.4) and sends its own correlation id on every
 * call (Norma 5.7.3). It retries only what a retry can fix: the network, 429 and 5xx, with
 * exponential backoff and full jitter (Annex D); a 4xx is an error of the caller and is never
 * retried.
 */
@Component
public class BookingExpireHoldsClient implements BookingExpireHoldsApi {

    private static final Logger log = LoggerFactory.getLogger(BookingExpireHoldsClient.class);
    private static final String OPERATION = "/internal/maintenance/expire-holds";
    private static final int MAX_ATTEMPTS = 3;
    private static final long BACKOFF_BASE_MILLIS = 200;

    private final HttpClient httpClient;
    private final ObjectMapper json = new ObjectMapper();
    private final String operationUrl;
    private final String serviceToken;
    private final Duration timeout;

    /**
     * @param bookingApiUrl the base of the booking service, with its context path: the service
     *     answers under {@code /api/v1/booking}, so the operation is {@code <base>/internal/...}
     */
    public BookingExpireHoldsClient(
            @Value("${booking.api.url:http://booking-api:8083/api/v1/booking}") String bookingApiUrl,
            @Value("${SERVICE_TOKEN:}") String serviceToken,
            @Value("${booking.api.timeout-seconds:10}") long timeoutSeconds) {
        if (serviceToken == null || serviceToken.isBlank()) {
            // Failing at boot is cheaper than a sweep that answers 401 every minute unnoticed.
            throw new IllegalStateException(
                    "SERVICE_TOKEN is required: the booking service rejects an unauthenticated sweep");
        }
        this.operationUrl = bookingApiUrl.replaceAll("/+$", "") + OPERATION;
        this.serviceToken = serviceToken;
        this.timeout = Duration.ofSeconds(timeoutSeconds);
        this.httpClient = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    @Override
    public ExpireHoldsResult expireHolds(String correlationId) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(operationUrl))
                .header("Authorization", "Bearer " + serviceToken)
                .header("X-Correlation-Id", correlationId)
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        for (int attempt = 1; ; attempt++) {
            HttpResponse<String> response;
            try {
                response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (IOException exception) {
                if (attempt == MAX_ATTEMPTS) {
                    throw new IllegalStateException("the booking service is unreachable", exception);
                }
                backOff(attempt, "network error");
                continue;
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while calling the booking service", exception);
            }
            int status = response.statusCode();
            if (status == 200) {
                return parse(response.body());
            }
            if ((status == 429 || status >= 500) && attempt < MAX_ATTEMPTS) {
                backOff(attempt, "status " + status);
                continue;
            }
            throw new IllegalStateException("the booking service answered " + status);
        }
    }

    private ExpireHoldsResult parse(String body) {
        try {
            JsonNode node = json.readTree(body);
            return new ExpireHoldsResult(node.path("expired").asInt(0), node.path("remaining").asInt(0));
        } catch (IOException exception) {
            throw new IllegalStateException("the answer of the booking service is not valid JSON", exception);
        }
    }

    /** Full jitter: a random wait between 0 and {@code base * 2^attempt} (Annex D). */
    private static void backOff(int attempt, String reason) {
        long ceiling = BACKOFF_BASE_MILLIS * (1L << attempt);
        long wait = ThreadLocalRandom.current().nextLong(ceiling + 1);
        log.warn("booking service call failed ({}), attempt {}/{}, retrying in {} ms", reason, attempt,
                MAX_ATTEMPTS, wait);
        try {
            Thread.sleep(wait);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}

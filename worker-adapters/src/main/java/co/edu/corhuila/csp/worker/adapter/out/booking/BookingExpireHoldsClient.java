package co.edu.corhuila.csp.worker.adapter.out.booking;

import co.edu.corhuila.csp.worker.application.port.out.BookingExpireHoldsApi;
import co.edu.corhuila.csp.worker.application.port.out.ExpireHoldsResult;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * HTTP client for the internal maintenance endpoint of the booking service. The worker
 * authenticates with its service token (Norma 5.7.4) and sends its own correlation id on every call
 * (Norma 5.7.3).
 */
@Component
public class BookingExpireHoldsClient implements BookingExpireHoldsApi {

    private static final Logger log = LoggerFactory.getLogger(BookingExpireHoldsClient.class);

    private final HttpClient httpClient;
    private final String bookingApiUrl;
    private final String serviceToken;
    private final Duration timeout;

    public BookingExpireHoldsClient(
            @Value("${booking.api.url:http://booking-api:8080}") String bookingApiUrl,
            @Value("${SERVICE_TOKEN:}") String serviceToken,
            @Value("${booking.api.timeout-seconds:10}") long timeoutSeconds) {
        if (serviceToken == null || serviceToken.isBlank()) {
            // Failing at boot is cheaper than a sweep that answers 401 every minute unnoticed.
            throw new IllegalStateException("SERVICE_TOKEN is required: the booking service rejects an unauthenticated sweep");
        }
        this.bookingApiUrl = bookingApiUrl;
        this.serviceToken = serviceToken;
        this.timeout = Duration.ofSeconds(timeoutSeconds);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .build();
    }

    @Override
    public ExpireHoldsResult expireHolds(String correlationId) {
        String url = bookingApiUrl + "/internal/maintenance/expire-holds";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + serviceToken)
                .header("X-Correlation-Id", correlationId)
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        int attempts = 0;
        int maxAttempts = 3;
        while (attempts < maxAttempts) {
            attempts++;
            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    return parseResponse(response.body());
                }
                if (response.statusCode() == 429 || response.statusCode() >= 500) {
                    log.warn("booking api returned {}, attempt {}/{}", response.statusCode(), attempts, maxAttempts);
                    sleepBeforeRetry(attempts);
                    continue;
                }
                throw new RuntimeException("booking api returned " + response.statusCode());
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted while calling booking api", exception);
            } catch (Exception exception) {
                if (attempts >= maxAttempts) {
                    throw new RuntimeException("failed to call booking api after " + attempts + " attempts", exception);
                }
                sleepBeforeRetry(attempts);
            }
        }
        throw new RuntimeException("failed to call booking api after " + attempts + " attempts");
    }

    private ExpireHoldsResult parseResponse(String body) {
        // Simple JSON parsing without external dependencies
        int expired = extractInt(body, "expired");
        int remaining = extractInt(body, "remaining");
        return new ExpireHoldsResult(expired, remaining);
    }

    private static int extractInt(String json, String field) {
        String pattern = "\"" + field + "\":";
        int index = json.indexOf(pattern);
        if (index < 0) {
            return 0;
        }
        int start = index + pattern.length();
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
            end++;
        }
        return Integer.parseInt(json.substring(start, end).trim());
    }

    /**
     * Exponential backoff with full jitter before a retry (Norma 5.7.2). The wait before attempt
     * {@code n} is a random value between 0 and {@code base * 2^n}.
     */
    private static void sleepBeforeRetry(int attempt) {
        long base = 200;
        long maxWait = base * (1L << Math.min(attempt, 10));
        long wait = (long) (Math.random() * maxWait);
        try {
            Thread.sleep(wait);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}

package co.edu.corhuila.csp.worker.adapter.out.auth;

import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.adapter.out.http.RetryingHttp;
import co.edu.corhuila.csp.worker.application.port.out.AuthMaintenanceApi;
import co.edu.corhuila.csp.worker.application.port.out.PurgeResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * HTTP client for the internal purge operation of the identity service (ADR-030). The worker
 * authenticates with its service token (Norma 5.7.4) and sends the correlation id of the run on
 * every call (Norma 5.7.3). It retries only what a retry can fix, as {@link RetryingHttp} does.
 */
@Component
@ConditionalOnProperty(prefix = "worker.purge-auth", name = "enabled", havingValue = "true")
public class AuthPurgeClient implements AuthMaintenanceApi {

    private static final String OPERATION = "/internal/maintenance/purge";

    private final RetryingHttp http;
    private final ObjectMapper json = new ObjectMapper();
    private final String operationUrl;
    private final ServiceTokenSource serviceToken;
    private final Duration timeout;

    /**
     * @param authApiUrl the base of the identity service, with its context path ({@code /api/v1/auth})
     */
    public AuthPurgeClient(
            @Value("${auth.api.url:http://auth-api:8081/api/v1/auth}") String authApiUrl,
            ServiceTokenSource serviceToken,
            @Value("${auth.api.timeout-seconds:10}") long timeoutSeconds) {
        this.operationUrl = authApiUrl.replaceAll("/+$", "") + OPERATION;
        this.serviceToken = serviceToken;
        this.timeout = Duration.ofSeconds(timeoutSeconds);
        this.http = new RetryingHttp(HttpClient.newBuilder().connectTimeout(timeout).build(), "identity service");
    }

    @Override
    public PurgeResult purge(String correlationId, int batchSize) {
        // The token is read now, not at startup: it has a lifetime and the platform may rotate it.
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(operationUrl))
                .header("Authorization", "Bearer " + serviceToken.current())
                .header("X-Correlation-Id", correlationId)
                .header("Content-Type", "application/json")
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.ofString(body(batchSize)))
                .build();
        return parse(http.send(request));
    }

    private String body(int batchSize) {
        try {
            return json.writeValueAsString(Map.of("batchSize", batchSize));
        } catch (IOException exception) {
            throw new IllegalStateException("the purge request cannot be written", exception);
        }
    }

    private PurgeResult parse(String body) {
        try {
            JsonNode node = json.readTree(body);
            return new PurgeResult(node.path("processedCount").asInt(0), node.path("hasMore").asBoolean(false));
        } catch (IOException exception) {
            throw new IllegalStateException("the answer of the identity service is not valid JSON", exception);
        }
    }
}

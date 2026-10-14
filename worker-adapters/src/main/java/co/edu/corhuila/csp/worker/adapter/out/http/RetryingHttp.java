package co.edu.corhuila.csp.worker.adapter.out.http;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends a request to another service of the platform and retries only what a retry can fix: the
 * network, 429 and 5xx, with exponential backoff and full jitter (Annex D). A 4xx is an error of
 * the caller and is never retried. Shared by the clients of the worker.
 */
public class RetryingHttp {

    private static final Logger log = LoggerFactory.getLogger(RetryingHttp.class);
    private static final int MAX_ATTEMPTS = 3;
    private static final long BACKOFF_BASE_MILLIS = 200;

    private final HttpClient httpClient;
    private final String service;

    /** @param service how logs and errors name the target, for example {@code "booking service"} */
    public RetryingHttp(HttpClient httpClient, String service) {
        this.httpClient = httpClient;
        this.service = service;
    }

    /**
     * @return the body of the 200 answer
     * @throws IllegalStateException when the service is unreachable, answers a status that is not
     *     worth retrying, or keeps failing after the last attempt
     */
    public String send(HttpRequest request) {
        for (int attempt = 1; ; attempt++) {
            HttpResponse<String> response;
            try {
                response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (IOException exception) {
                if (attempt == MAX_ATTEMPTS) {
                    throw new IllegalStateException("the " + service + " is unreachable", exception);
                }
                backOff(attempt, "network error");
                continue;
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while calling the " + service, exception);
            }
            int status = response.statusCode();
            if (status == 200) {
                return response.body();
            }
            if ((status == 429 || status >= 500) && attempt < MAX_ATTEMPTS) {
                backOff(attempt, "status " + status);
                continue;
            }
            if (status == 401 || status == 403) {
                log.error("the {} refused the service token (status {}): it may have expired or its"
                        + " subject is not allowed; replace the secret of the worker", service, status);
            }
            throw new IllegalStateException("the " + service + " answered " + status);
        }
    }

    /** Full jitter: a random wait between 0 and {@code base * 2^attempt} (Annex D). */
    private void backOff(int attempt, String reason) {
        long ceiling = BACKOFF_BASE_MILLIS * (1L << attempt);
        long wait = ThreadLocalRandom.current().nextLong(ceiling + 1);
        log.warn("{} call failed ({}), attempt {}/{}, retrying in {} ms", service, reason, attempt,
                MAX_ATTEMPTS, wait);
        try {
            Thread.sleep(wait);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}

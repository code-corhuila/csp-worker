package co.edu.corhuila.csp.worker.adapter.out.booking;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Where the service token of the worker comes from. A token has a lifetime, so it is read at every
 * sweep and not once at startup: with {@code SERVICE_TOKEN_FILE} the file is read again each time,
 * which lets the platform rotate the secret without restarting the worker; with
 * {@code SERVICE_TOKEN} the value is fixed for the life of the process. One of the two is required,
 * and the worker refuses to start without a token (Norma 5.7.4).
 */
@Component
@ConditionalOnProperty(prefix = "worker.expire-holds", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ServiceTokenSource {

    private final String fixedToken;
    private final Path tokenFile;

    public ServiceTokenSource(@Value("${SERVICE_TOKEN:}") String token,
            @Value("${SERVICE_TOKEN_FILE:}") String file) {
        this.fixedToken = token == null ? "" : token.strip();
        this.tokenFile = file == null || file.isBlank() ? null : Path.of(file.strip());
        // Failing at boot is cheaper than a sweep that answers 401 every minute unnoticed.
        current();
    }

    /** The token to send now. */
    public String current() {
        String token = tokenFile == null ? fixedToken : readFile();
        if (token.isBlank()) {
            throw new IllegalStateException(
                    "SERVICE_TOKEN or SERVICE_TOKEN_FILE is required: the booking service rejects an unauthenticated sweep");
        }
        return token;
    }

    private String readFile() {
        try {
            return Files.readString(tokenFile, StandardCharsets.UTF_8).strip();
        } catch (IOException exception) {
            throw new IllegalStateException("the service token file cannot be read: " + tokenFile, exception);
        }
    }
}

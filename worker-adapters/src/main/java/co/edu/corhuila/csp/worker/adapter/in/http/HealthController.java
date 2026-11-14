package co.edu.corhuila.csp.worker.adapter.in.http;

import co.edu.corhuila.csp.worker.application.usecase.ReadinessCheck;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The only HTTP point of the worker: a health endpoint (Norma 5.7.1), with no business interface. {@code /health}
 * has no dependencies and only says that the process answers; {@code /health/ready} asks PostgreSQL and RabbitMQ
 * for the relays that are enabled (ADR-014) and answers {@code 503} when one of them does not answer. Neither
 * says anything beyond up or down: no address, no login and no error text.
 */
@RestController
public class HealthController {

    private static final String UP = "UP";
    private static final String DOWN = "DOWN";

    private final ReadinessCheck readiness;

    public HealthController(ReadinessCheck readiness) {
        this.readiness = readiness;
    }

    @GetMapping("/health")
    ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(Map.of("status", UP));
    }

    @GetMapping("/health/ready")
    ResponseEntity<Map<String, Object>> ready() {
        ReadinessCheck.Report report = readiness.check();
        Map<String, String> dependencies = new LinkedHashMap<>();
        report.dependencies().forEach((name, reachable) -> dependencies.put(name, reachable ? UP : DOWN));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", report.ready() ? UP : DOWN);
        body.put("dependencies", dependencies);
        HttpStatus status = report.ready() ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(status).cacheControl(CacheControl.noStore()).body(body);
    }
}

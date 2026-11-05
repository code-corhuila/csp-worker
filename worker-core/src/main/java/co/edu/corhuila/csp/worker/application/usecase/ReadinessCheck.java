package co.edu.corhuila.csp.worker.application.usecase;

import co.edu.corhuila.csp.worker.application.port.out.DependencyProbe;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Whether the worker can do its work: PostgreSQL and RabbitMQ answer for every relay that is enabled (ADR-014).
 * Several probes of the same kind, such as the relays of two domains, are one answer: the kind is ready only
 * when all of them are. A worker with no relay enabled depends on neither and is ready.
 */
public final class ReadinessCheck {

    private final List<DependencyProbe> probes;

    public ReadinessCheck(List<DependencyProbe> probes) {
        this.probes = List.copyOf(probes);
    }

    public Report check() {
        Map<String, Boolean> byKind = new TreeMap<>();
        for (DependencyProbe probe : probes) {
            byKind.merge(probe.dependency(), probe.isReachable(), Boolean::logicalAnd);
        }
        return new Report(Collections.unmodifiableMap(byKind));
    }

    /** The answer of each kind of dependency, by name. */
    public record Report(Map<String, Boolean> dependencies) {

        public boolean ready() {
            return dependencies.values().stream().allMatch(Boolean::booleanValue);
        }
    }
}

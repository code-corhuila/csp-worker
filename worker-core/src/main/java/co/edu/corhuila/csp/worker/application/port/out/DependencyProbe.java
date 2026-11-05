package co.edu.corhuila.csp.worker.application.port.out;

/**
 * A dependency without which the worker cannot do its work, asked by the readiness check (ADR-014). The
 * answer is a plain yes or no: a probe never throws and never takes longer than its own timeout.
 */
public interface DependencyProbe {

    /** The kind of dependency, as the readiness answer names it: {@code postgresql} or {@code rabbitmq}. */
    String dependency();

    /** True when the dependency answers now. */
    boolean isReachable();
}

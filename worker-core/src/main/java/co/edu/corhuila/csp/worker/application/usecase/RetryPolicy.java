package co.edu.corhuila.csp.worker.application.usecase;

import java.time.Duration;
import java.util.function.DoubleSupplier;

/**
 * The retry schedule of the relay (ADR-014): {@code min(5 minutes, 1 second * 2^(attempt-1))}
 * moved by a random jitter of plus or minus 20 percent, and 20 attempts at most.
 */
public class RetryPolicy {

    static final int MAX_ATTEMPTS = 20;
    private static final Duration FIRST_DELAY = Duration.ofSeconds(1);
    private static final Duration CAP = Duration.ofMinutes(5);
    private static final double JITTER = 0.2;

    private final DoubleSupplier random;

    /** @param random a value in [0, 1) per call; a test passes a constant */
    public RetryPolicy(DoubleSupplier random) {
        this.random = random;
    }

    /** The wait after the {@code attempt}-th failed attempt, counted from 1. */
    public Duration delay(int attempt) {
        long exponent = Math.min(attempt - 1, 30);
        long base = Math.min(FIRST_DELAY.toMillis() << exponent, CAP.toMillis());
        double factor = 1 + (random.getAsDouble() * 2 - 1) * JITTER;
        return Duration.ofMillis(Math.min(Math.round(base * factor), CAP.toMillis()));
    }

    public boolean isExhausted(int attempts) {
        return attempts >= MAX_ATTEMPTS;
    }
}

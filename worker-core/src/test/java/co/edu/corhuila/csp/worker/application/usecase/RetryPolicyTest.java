package co.edu.corhuila.csp.worker.application.usecase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The retry schedule of ADR-014: {@code min(5 minutes, 1 second * 2^(attempt-1))} with a jitter of
 * plus or minus 20 percent, and at most 20 attempts.
 */
class RetryPolicyTest {

    private final RetryPolicy withoutJitter = new RetryPolicy(() -> 0.5);

    @Test
    void theDelayDoublesFromOneSecond() {
        assertEquals(Duration.ofSeconds(1), withoutJitter.delay(1));
        assertEquals(Duration.ofSeconds(2), withoutJitter.delay(2));
        assertEquals(Duration.ofSeconds(4), withoutJitter.delay(3));
        assertEquals(Duration.ofSeconds(128), withoutJitter.delay(8));
    }

    @Test
    void theDelayNeverPassesFiveMinutes() {
        assertEquals(Duration.ofMinutes(5), withoutJitter.delay(10));
        assertEquals(Duration.ofMinutes(5), withoutJitter.delay(20));
        assertEquals(Duration.ofMinutes(5), withoutJitter.delay(1_000));
    }

    @Test
    void theJitterMovesTheDelayByAtMostTwentyPercent() {
        assertEquals(Duration.ofMillis(6_400), new RetryPolicy(() -> 0.0).delay(4));
        assertEquals(Duration.ofMillis(9_600), new RetryPolicy(() -> 1.0).delay(4));
    }

    @Test
    void theJitterNeverPushesTheDelayOverTheCap() {
        assertEquals(Duration.ofMinutes(5), new RetryPolicy(() -> 1.0).delay(15));
    }

    @Test
    void anEventGetsTwentyAttemptsAndNoMore() {
        assertFalse(withoutJitter.isExhausted(19));
        assertTrue(withoutJitter.isExhausted(20));
        assertTrue(withoutJitter.isExhausted(21));
    }
}

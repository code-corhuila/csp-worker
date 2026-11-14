package co.edu.corhuila.csp.worker.application.usecase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.application.port.out.DependencyProbe;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ReadinessCheckTest {

    private static DependencyProbe probe(String dependency, boolean reachable) {
        return new DependencyProbe() {
            @Override
            public String dependency() {
                return dependency;
            }

            @Override
            public boolean isReachable() {
                return reachable;
            }
        };
    }

    @Test
    void aWorkerWithNoRelayDependsOnNothingAndIsReady() {
        ReadinessCheck.Report report = new ReadinessCheck(List.of()).check();

        assertTrue(report.ready());
        assertTrue(report.dependencies().isEmpty());
    }

    @Test
    void itIsReadyWhenEveryDependencyAnswers() {
        ReadinessCheck.Report report = new ReadinessCheck(
                List.of(probe("postgresql", true), probe("rabbitmq", true))).check();

        assertTrue(report.ready());
        assertEquals(Map.of("postgresql", true, "rabbitmq", true), report.dependencies());
    }

    @Test
    void itIsNotReadyWhenOneDependencyDoesNotAnswer() {
        ReadinessCheck.Report report = new ReadinessCheck(
                List.of(probe("postgresql", true), probe("rabbitmq", false))).check();

        assertFalse(report.ready());
        assertEquals(Map.of("postgresql", true, "rabbitmq", false), report.dependencies());
    }

    @Test
    void severalProbesOfTheSameKindAreOneAnswerThatNeedsAllOfThem() {
        ReadinessCheck.Report report = new ReadinessCheck(
                List.of(probe("postgresql", true), probe("postgresql", false), probe("rabbitmq", true))).check();

        assertFalse(report.ready());
        assertEquals(Map.of("postgresql", false, "rabbitmq", true), report.dependencies());
    }
}

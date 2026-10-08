package co.edu.corhuila.csp.worker.adapter.in.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.application.port.in.Job;
import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * What the scheduler promises to the jobs it runs: every job has its own thread and its own
 * interval, so a slow relay never delays the expiration sweep; a failing job keeps being scheduled
 * and does not stop the others; and a job never runs twice at the same time.
 */
class JobRunnerTest {

    private static final Duration SHORT = Duration.ofMillis(10);

    private final CountDownLatch release = new CountDownLatch(1);
    private JobRunner runner;

    @AfterEach
    void stopTheRunner() {
        // a job that waits for the latch would make the shutdown wait for its 30 seconds
        release.countDown();
        if (runner != null) {
            runner.shutdown();
        }
    }

    private static Job job(String name, Runnable body) {
        return new Job() {
            @Override
            public JobResult run() {
                body.run();
                return new JobResult(1, 0);
            }

            @Override
            public String name() {
                return name;
            }
        };
    }

    private static boolean eventually(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(5);
        }
        return condition.getAsBoolean();
    }

    @Test
    void aSlowJobDoesNotDelayTheOtherOne() throws Exception {
        CountDownLatch slowStarted = new CountDownLatch(1);
        AtomicInteger fastRuns = new AtomicInteger();
        Job slow = job("slow", () -> {
            slowStarted.countDown();
            try {
                release.await();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        });
        runner = new JobRunner(List.of(new ScheduledJob(slow, SHORT), new ScheduledJob(job("fast", fastRuns::incrementAndGet), SHORT)));

        runner.start();

        assertTrue(slowStarted.await(3, TimeUnit.SECONDS), "the slow job must have started");
        assertTrue(eventually(() -> fastRuns.get() >= 3), "the fast job kept running while the slow one was blocked");
    }

    @Test
    void aFailingJobKeepsBeingScheduledAndDoesNotStopTheOthers() throws Exception {
        AtomicInteger failingRuns = new AtomicInteger();
        AtomicInteger healthyRuns = new AtomicInteger();
        Job failing = job("failing", () -> {
            failingRuns.incrementAndGet();
            throw new IllegalStateException("the service it calls is down");
        });
        runner = new JobRunner(List.of(new ScheduledJob(failing, SHORT), new ScheduledJob(job("healthy", healthyRuns::incrementAndGet), SHORT)));

        runner.start();

        assertTrue(eventually(() -> failingRuns.get() >= 3), "a failure must not cancel the next runs of the same job");
        assertTrue(eventually(() -> healthyRuns.get() >= 3), "a failure must not stop the other job");
    }

    @Test
    void aJobNeverRunsTwiceAtTheSameTime() throws Exception {
        AtomicInteger running = new AtomicInteger();
        AtomicInteger mostAtOnce = new AtomicInteger();
        AtomicInteger runs = new AtomicInteger();
        Job slowish = job("slowish", () -> {
            int now = running.incrementAndGet();
            mostAtOnce.accumulateAndGet(now, Math::max);
            try {
                Thread.sleep(25);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
            running.decrementAndGet();
            runs.incrementAndGet();
        });
        runner = new JobRunner(List.of(new ScheduledJob(slowish, Duration.ofMillis(1))));

        runner.start();

        assertTrue(eventually(() -> runs.get() >= 4));
        assertEquals(1, mostAtOnce.get());
    }

    @Test
    void anIntervalThatIsNotPositiveIsRefused() {
        Job any = job("any", () -> { });

        assertThrows(IllegalArgumentException.class, () -> new ScheduledJob(any, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new ScheduledJob(any, Duration.ofSeconds(-1)));
    }
}

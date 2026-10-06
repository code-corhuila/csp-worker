package co.edu.corhuila.csp.worker.adapter.in.scheduler;

import co.edu.corhuila.csp.worker.application.port.in.Job;
import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * The scheduler of the worker: runs each job with a fixed delay between the end of one run and the start of the next, so a slow run under a degraded service is never followed at once by another. Each run is bounded (Norma 5.7):
 * the job itself limits its batch size and the scheduler runs one at a time.
 */
@Component
public class JobRunner {

    private static final Logger log = LoggerFactory.getLogger(JobRunner.class);

    private final List<Job> jobs;
    private final long intervalSeconds;
    private final ScheduledExecutorService executor;

    public JobRunner(List<Job> jobs,
            @Value("${worker.expire-holds.interval-seconds:60}") long intervalSeconds) {
        this.jobs = jobs;
        this.intervalSeconds = intervalSeconds;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "worker-scheduler");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Starts the scheduler. Called once at application startup.
     */
    public void start() {
        log.info("worker scheduler started with {} jobs, interval={}s", jobs.size(), intervalSeconds);
        executor.scheduleWithFixedDelay(this::runAll, 0, intervalSeconds, TimeUnit.SECONDS);
    }

    /**
     * Runs all jobs once. A failure in one job does not stop the others (Norma 5.7).
     */
    private void runAll() {
        for (Job job : jobs) {
            try {
                job.run();
            } catch (Exception exception) {
                log.error("job {} failed", job.name(), exception);
            }
        }
    }

    /**
     * Stops the scheduler when the application stops: a sweep in flight is given time to finish
     * instead of being abandoned in the middle of its HTTP call.
     */
    @PreDestroy
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException exception) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

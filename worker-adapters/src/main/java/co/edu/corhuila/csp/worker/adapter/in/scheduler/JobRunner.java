package co.edu.corhuila.csp.worker.adapter.in.scheduler;

import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The scheduler of the worker: runs each job with a fixed delay between the end of one run and the start of the next, so a slow run under a degraded service is never followed at once by another. Each job has its own interval and its own thread, so a slow relay never delays the expiration sweep. Each run is bounded (Norma 5.7):
 * the job itself limits its batch size and a job never runs twice at the same time.
 */
@Component
public class JobRunner {

    private static final Logger log = LoggerFactory.getLogger(JobRunner.class);

    private final List<ScheduledJob> jobs;
    private final ScheduledExecutorService executor;

    public JobRunner(List<ScheduledJob> jobs) {
        this.jobs = jobs;
        this.executor = Executors.newScheduledThreadPool(Math.max(1, jobs.size()), runnable -> {
            Thread thread = new Thread(runnable, "worker-scheduler");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Starts the scheduler. Called once at application startup.
     */
    public void start() {
        for (ScheduledJob scheduled : jobs) {
            log.info("worker scheduler started job {}, interval={}s", scheduled.job().name(),
                    scheduled.interval().toSeconds());
            executor.scheduleWithFixedDelay(() -> run(scheduled), 0, scheduled.interval().toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Runs one job once. A failure in a job does not stop the scheduler or the other jobs (Norma 5.7).
     */
    private void run(ScheduledJob scheduled) {
        try {
            scheduled.job().run();
        } catch (Exception exception) {
            log.error("job {} failed", scheduled.job().name(), exception);
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

package co.edu.corhuila.csp.worker.app;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.JobRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

/**
 * The csp-worker application: runs scheduled background jobs (Norma 4.3). It exposes no business
 * interface, only a health endpoint (Norma 5.7.1).
 */
// The adapters live beside the application, not under it: both are scanned on purpose.
@SpringBootApplication(scanBasePackages = {
        "co.edu.corhuila.csp.worker.app",
        "co.edu.corhuila.csp.worker.adapter"})
public class WorkerApplication {

    private final JobRunner jobRunner;

    public WorkerApplication(JobRunner jobRunner) {
        this.jobRunner = jobRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startScheduler() {
        jobRunner.start();
    }
}

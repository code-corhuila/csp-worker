package co.edu.corhuila.csp.worker.application.port.in;

/**
 * One scheduled unit of work. The scheduler asks each job to run and reports how many items it
 * processed and how many failed.
 */
public interface Job {

    /**
     * Executes one run of this job.
     *
     * @return the outcome of the run: how many items were processed and how many failed
     */
    JobResult run();

    /**
     * The name of this job, used in logs and metrics.
     */
    String name();
}

package co.edu.corhuila.csp.worker.adapter.in.scheduler;

import co.edu.corhuila.csp.worker.application.port.in.Job;
import java.time.Duration;

/** A job and how long the scheduler waits between the end of a run and the start of the next. */
public record ScheduledJob(Job job, Duration interval) {

    public ScheduledJob {
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("the interval of " + job.name() + " must be positive");
        }
    }
}

package co.edu.corhuila.csp.worker.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.JobRunner;
import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

/**
 * Each job has its own flag, so the same image runs as the instances a platform needs: one with
 * the light jobs and another with the heavy ones. An instance without the expiration sweep does
 * not talk to the booking service and so it does not need its service token either.
 */
@SpringBootTest(properties = {
        "worker.expire-holds.enabled=false",
        "SERVICE_TOKEN=",
        "SERVICE_TOKEN_FILE=",
        "server.port=0"})
class JobFlagsWiringTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void anInstanceWithEveryJobOffStartsWithoutJobsAndWithoutAServiceToken() {
        assertTrue(context.getBeansOfType(ScheduledJob.class).isEmpty());
        assertTrue(context.getBeansOfType(ServiceTokenSource.class).isEmpty());
        assertEquals(1, context.getBeansOfType(JobRunner.class).size());
    }
}

package co.edu.corhuila.csp.worker.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import co.edu.corhuila.csp.worker.adapter.out.booking.BookingExpireHoldsClient;
import co.edu.corhuila.csp.worker.application.port.in.Job;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

/** The application wires its jobs: the scheduler is only as good as the jobs it is given. */
@SpringBootTest(properties = {
        "SERVICE_TOKEN=test-token",
        "server.port=0",
        "worker.expire-holds.interval-seconds=3600"})
class WorkerWiringTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void theExpireHoldsJobIsRegisteredForTheScheduler() {
        Map<String, Job> jobs = context.getBeansOfType(Job.class);

        assertEquals(1, jobs.size());
        assertEquals("expire-holds", jobs.values().iterator().next().name());
    }

    @Test
    void aMissingServiceTokenStopsTheWorkerAtStartup() {
        assertThrows(IllegalStateException.class,
                () -> new BookingExpireHoldsClient("http://booking-api:8080", " ", 10));
    }
}

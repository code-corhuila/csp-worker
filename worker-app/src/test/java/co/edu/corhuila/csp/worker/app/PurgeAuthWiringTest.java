package co.edu.corhuila.csp.worker.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import co.edu.corhuila.csp.worker.adapter.out.auth.AuthPurgeClient;
import co.edu.corhuila.csp.worker.adapter.out.booking.BookingExpireHoldsClient;
import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

/** An instance that only purges the identity service: it has its own job, client and service token. */
@SpringBootTest(properties = {
        "worker.expire-holds.enabled=false",
        "worker.purge-auth.enabled=true",
        "worker.purge-auth.interval-seconds=7200",
        "SERVICE_TOKEN=test-token",
        "server.port=0"})
class PurgeAuthWiringTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void thePurgeJobIsTheOnlyOneRegisteredWithItsClientAndServiceToken() {
        Map<String, ScheduledJob> jobs = context.getBeansOfType(ScheduledJob.class);

        assertEquals(1, jobs.size());
        ScheduledJob scheduled = jobs.values().iterator().next();
        assertEquals("purge-auth", scheduled.job().name());
        assertEquals(Duration.ofHours(2), scheduled.interval());
        assertEquals(1, context.getBeansOfType(AuthPurgeClient.class).size());
        assertEquals(1, context.getBeansOfType(ServiceTokenSource.class).size());
        assertEquals(0, context.getBeansOfType(BookingExpireHoldsClient.class).size());
    }
}

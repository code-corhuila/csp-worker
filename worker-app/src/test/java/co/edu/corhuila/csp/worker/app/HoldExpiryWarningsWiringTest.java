package co.edu.corhuila.csp.worker.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import co.edu.corhuila.csp.worker.adapter.out.booking.BookingExpireHoldsClient;
import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.adapter.out.ticketing.TicketingHoldExpiryWarningsClient;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

/** An instance that only warns about the holds about to expire: its own job, the Ticketing client and a token. */
@SpringBootTest(properties = {
        "worker.expire-holds.enabled=false",
        "worker.hold-expiry-warnings.enabled=true",
        "worker.hold-expiry-warnings.interval-seconds=15",
        "SERVICE_TOKEN=test-token",
        "server.port=0"})
class HoldExpiryWarningsWiringTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void theWarningsAreTheOnlyJobRegisteredWithTheTicketingClientAndTheServiceToken() {
        Map<String, ScheduledJob> jobs = context.getBeansOfType(ScheduledJob.class);

        assertEquals(1, jobs.size());
        ScheduledJob scheduled = jobs.values().iterator().next();
        assertEquals("hold-expiry-warnings", scheduled.job().name());
        assertEquals(Duration.ofSeconds(15), scheduled.interval());
        assertEquals(1, context.getBeansOfType(TicketingHoldExpiryWarningsClient.class).size());
        assertEquals(1, context.getBeansOfType(ServiceTokenSource.class).size());
        assertEquals(0, context.getBeansOfType(BookingExpireHoldsClient.class).size());
    }
}

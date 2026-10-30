package co.edu.corhuila.csp.worker.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import co.edu.corhuila.csp.worker.adapter.out.booking.BookingExpireHoldsClient;
import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.adapter.out.ticketing.TicketingDispatchClient;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

/** An instance that only dispatches the ticket emails: it has its own job, the Ticketing client and a service token. */
@SpringBootTest(properties = {
        "worker.expire-holds.enabled=false",
        "worker.dispatch-ticket-emails.enabled=true",
        "worker.dispatch-ticket-emails.interval-seconds=15",
        "SERVICE_TOKEN=test-token",
        "server.port=0"})
class DispatchTicketEmailsWiringTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void theDispatchIsTheOnlyJobRegisteredWithTheTicketingClientAndTheServiceToken() {
        Map<String, ScheduledJob> jobs = context.getBeansOfType(ScheduledJob.class);

        assertEquals(1, jobs.size());
        ScheduledJob scheduled = jobs.values().iterator().next();
        assertEquals("dispatch-ticket-emails", scheduled.job().name());
        assertEquals(Duration.ofSeconds(15), scheduled.interval());
        assertEquals(1, context.getBeansOfType(TicketingDispatchClient.class).size());
        assertEquals(1, context.getBeansOfType(ServiceTokenSource.class).size());
        assertEquals(0, context.getBeansOfType(BookingExpireHoldsClient.class).size());
    }
}

package co.edu.corhuila.csp.worker.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import co.edu.corhuila.csp.worker.adapter.out.booking.BookingExpireHoldsClient;
import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.adapter.out.concessions.ConcessionsOrdersClient;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

/** An instance that only sweeps the orders of Concessions: it has its own job, the Concessions client and a service token. */
@SpringBootTest(properties = {
        "worker.expire-holds.enabled=false",
        "worker.expire-concessions-orders.enabled=true",
        "worker.expire-concessions-orders.interval-seconds=120",
        "SERVICE_TOKEN=test-token",
        "server.port=0"})
class ExpireConcessionsOrdersWiringTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void theSweepIsTheOnlyJobRegisteredWithTheConcessionsClientAndTheServiceToken() {
        Map<String, ScheduledJob> jobs = context.getBeansOfType(ScheduledJob.class);

        assertEquals(1, jobs.size());
        ScheduledJob scheduled = jobs.values().iterator().next();
        assertEquals("expire-concessions-orders", scheduled.job().name());
        assertEquals(Duration.ofMinutes(2), scheduled.interval());
        assertEquals(1, context.getBeansOfType(ConcessionsOrdersClient.class).size());
        assertEquals(1, context.getBeansOfType(ServiceTokenSource.class).size());
        assertEquals(0, context.getBeansOfType(BookingExpireHoldsClient.class).size());
    }
}

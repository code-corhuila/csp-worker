package co.edu.corhuila.csp.worker.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import co.edu.corhuila.csp.worker.adapter.out.booking.BookingExpireHoldsClient;
import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.adapter.out.storage.S3TicketPdfStorage;
import co.edu.corhuila.csp.worker.adapter.out.ticketing.TicketingDispatchClient;
import co.edu.corhuila.csp.worker.adapter.out.ticketing.TicketingPdfClient;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

/**
 * An instance that only renders the ticket PDFs: it has its own job, the Ticketing client, the writer of the object
 * storage and a service token, and none of the other clients.
 */
@SpringBootTest(properties = {
        "worker.expire-holds.enabled=false",
        "worker.render-ticket-pdfs.enabled=true",
        "worker.render-ticket-pdfs.interval-seconds=20",
        "object-storage.url=http://127.0.0.1:1",
        "object-storage.access-key=ticketing_pdf_writer",
        "object-storage.secret-key=test-secret",
        "SERVICE_TOKEN=test-token",
        "server.port=0"})
class RenderTicketPdfsWiringTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void theRenderingIsTheOnlyJobRegisteredWithTheTicketingClientTheWriterAndTheServiceToken() {
        Map<String, ScheduledJob> jobs = context.getBeansOfType(ScheduledJob.class);

        assertEquals(1, jobs.size());
        ScheduledJob scheduled = jobs.values().iterator().next();
        assertEquals("render-ticket-pdfs", scheduled.job().name());
        assertEquals(Duration.ofSeconds(20), scheduled.interval());
        assertEquals(1, context.getBeansOfType(TicketingPdfClient.class).size());
        assertEquals(1, context.getBeansOfType(S3TicketPdfStorage.class).size());
        assertEquals(1, context.getBeansOfType(ServiceTokenSource.class).size());
        assertEquals(0, context.getBeansOfType(TicketingDispatchClient.class).size());
        assertEquals(0, context.getBeansOfType(BookingExpireHoldsClient.class).size());
    }
}

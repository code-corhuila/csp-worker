package co.edu.corhuila.csp.worker.application.usecase;

import co.edu.corhuila.csp.worker.application.port.in.Job;
import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.TicketPdfStorage;
import co.edu.corhuila.csp.worker.application.port.out.TicketingApiException;
import co.edu.corhuila.csp.worker.application.port.out.TicketingPdfApi;
import co.edu.corhuila.csp.worker.application.port.out.pdf.TicketPdfGeneratorPort;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The rendering of the ticket PDFs (HU-TICKETING-002, ADR-057 and ADR-058): asks the Ticketing service which tickets
 * still lack their document, renders each one, stores it in the private bucket under its immutable key and reports the
 * reference, which makes the service issue the ticket. A failure in one ticket does not stop the batch (Norma 5.7); the
 * ticket stays in the list and is tried again in the next run.
 *
 * <p>Safe to run twice (Norma 5.7, rule 1): the storage keeps the first object of a key, and the service refuses to
 * change a recorded reference, so a ticket whose report was lost is only reported again. A run is bounded by the batch
 * size, its time and a cap of calls.
 */
public class RenderTicketPdfsJob implements Job {

    /** The maximum limit of the contract of {@code /internal/tickets/awaiting-pdf}. */
    static final int MAX_BATCH_SIZE = 100;
    static final int MAX_CALLS_PER_RUN = 100;
    /** Norma 5.3.10: a ticket PDF is a page; anything bigger is a defect, not a document. */
    static final int MAX_PDF_BYTES = 1024 * 1024;

    private static final Logger log = LoggerFactory.getLogger(RenderTicketPdfsJob.class);

    private final TicketingPdfApi ticketing;
    private final TicketPdfGeneratorPort generator;
    private final TicketPdfStorage storage;
    private final int batchSize;
    private final Duration runTimeout;
    private final Clock clock;

    public RenderTicketPdfsJob(TicketingPdfApi ticketing, TicketPdfGeneratorPort generator, TicketPdfStorage storage,
            int batchSize, Duration runTimeout, Clock clock) {
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("the render batch size must be between 1 and " + MAX_BATCH_SIZE);
        }
        if (runTimeout.isZero() || runTimeout.isNegative()) {
            throw new IllegalArgumentException("the render run timeout must be positive");
        }
        this.ticketing = ticketing;
        this.generator = generator;
        this.storage = storage;
        this.batchSize = batchSize;
        this.runTimeout = runTimeout;
        this.clock = clock;
    }

    /** {@code tickets/{reservationId}/{ticketId}.pdf}, the key of ADR-058. */
    static String keyOf(TicketPdfData ticket) {
        return "tickets/" + ticket.reservationId() + "/" + ticket.ticketId() + ".pdf";
    }

    @Override
    public JobResult run() {
        String correlationId = UUID.randomUUID().toString();
        Instant deadline = clock.instant().plus(runTimeout);
        Set<UUID> failedInThisRun = new HashSet<>();
        int rendered = 0;
        for (int calls = 0; calls < MAX_CALLS_PER_RUN && clock.instant().isBefore(deadline); calls++) {
            List<TicketPdfData> batch;
            try {
                batch = ticketing.awaitingPdf(correlationId, batchSize);
            } catch (TicketingApiException exception) {
                log.error("render ticket pdfs failed to list: rendered={}, correlationId={}", rendered, correlationId,
                        exception);
                return new JobResult(rendered, failedInThisRun.size() + 1);
            }
            int renderedNow = 0;
            for (TicketPdfData ticket : batch) {
                if (failedInThisRun.contains(ticket.ticketId())) {
                    continue;
                }
                try {
                    render(ticket, correlationId);
                    renderedNow++;
                } catch (RuntimeException exception) {
                    failedInThisRun.add(ticket.ticketId());
                    log.error("render ticket pdf failed: ticketId={}, correlationId={}", ticket.ticketId(),
                            correlationId, exception);
                }
            }
            rendered += renderedNow;
            // A short batch is the last one; a batch where nothing advanced would only repeat itself.
            if (batch.size() < batchSize || renderedNow == 0) {
                break;
            }
        }
        log.info("render ticket pdfs completed: rendered={}, failed={}, correlationId={}", rendered,
                failedInThisRun.size(), correlationId);
        return new JobResult(rendered, failedInThisRun.size());
    }

    private void render(TicketPdfData ticket, String correlationId) {
        byte[] pdf = generator.generateTicketPdf(ticket);
        if (pdf.length > MAX_PDF_BYTES) {
            throw new IllegalStateException("the pdf of the ticket is " + pdf.length + " bytes, over " + MAX_PDF_BYTES);
        }
        String reference = storage.storeOnce(keyOf(ticket), pdf);
        ticketing.registerPdf(correlationId, ticket.ticketId(), reference);
    }

    @Override
    public String name() {
        return "render-ticket-pdfs";
    }
}

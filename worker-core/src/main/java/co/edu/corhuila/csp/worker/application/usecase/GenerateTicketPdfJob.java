package co.edu.corhuila.csp.worker.application.usecase;

import co.edu.corhuila.csp.worker.application.port.in.Job;
import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.EventPublisher;
import co.edu.corhuila.csp.worker.application.port.out.OutboxEvent;
import co.edu.corhuila.csp.worker.application.port.out.OutboxPosition;
import co.edu.corhuila.csp.worker.application.port.out.OutboxSource;
import co.edu.corhuila.csp.worker.application.port.out.PendingEvent;
import co.edu.corhuila.csp.worker.application.port.out.PublishException;
import co.edu.corhuila.csp.worker.application.port.out.RelayStateStore;
import co.edu.corhuila.csp.worker.application.port.out.RelayStatus;
import co.edu.corhuila.csp.worker.application.port.out.RunLock;
import co.edu.corhuila.csp.worker.application.port.out.pdf.TicketPdfGeneratorPort;
import co.edu.corhuila.csp.worker.application.port.out.storage.ObjectStoragePort;
import co.edu.corhuila.csp.worker.application.port.out.ticketing.TicketingApi;
import co.edu.corhuila.csp.worker.application.port.out.EnvelopeValidator;
import co.edu.corhuila.csp.worker.application.usecase.TicketPdfData;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenerateTicketPdfJob implements Job {

    private static final int BATCH = 50, MAX_ERROR_LENGTH = 500;
    private static final Duration RUN_LIMIT = Duration.ofSeconds(60), OVERLAP = Duration.ofMinutes(5);

    private static final Logger log = LoggerFactory.getLogger(GenerateTicketPdfJob.class);

    private final OutboxSource source;
    private final RelayStateStore state;
    private final EnvelopeValidator validator;
    private final EventPublisher publisher;
    private final RetryPolicy retryPolicy;
    private final Clock clock;
    private final RunLock lock;
    private final TicketPdfGeneratorPort pdfGenerator;
    private final ObjectStoragePort objectStorage;
    private final TicketingApi ticketingApi;
    private final String name;

    public GenerateTicketPdfJob(String name, OutboxSource source, RelayStateStore state,
                                EnvelopeValidator validator, EventPublisher publisher,
                                RetryPolicy retryPolicy, Clock clock, RunLock lock,
                                TicketPdfGeneratorPort pdfGenerator,
                                ObjectStoragePort objectStorage,
                                TicketingApi ticketingApi) {
        this.name = name; this.source = source; this.state = state;
        this.validator = validator; this.publisher = publisher; this.retryPolicy = retryPolicy;
        this.clock = clock; this.lock = lock; this.pdfGenerator = pdfGenerator;
        this.objectStorage = objectStorage; this.ticketingApi = ticketingApi;
    }

    @Override public JobResult run() {
        String runId = UUID.randomUUID().toString();
        Instant deadline = clock.instant().plus(RUN_LIMIT);
        try {
            var lease = lock.tryAcquire();
            if (lease.isEmpty()) { log.debug("{} run skipped, lock held", name); return new JobResult(0, 0); }
            try (var held = lease.get()) { discover(deadline); return publishDue(runId, deadline); }
        } catch (Exception e) { log.error("{} run failed: {}", name, runId, e); return new JobResult(0, 1); }
    }

    @Override public String name() { return name; }

    private void discover(Instant deadline) {
        var saved = state.cursor();
        var position = saved.map(c -> c.minus(OVERLAP)).orElse(OutboxPosition.START);
        OutboxPosition furthest = null;
        while (clock.instant().isBefore(deadline)) {
            var page = source.discover(position, BATCH);
            if (page.isEmpty()) break;
            state.track(page.stream().map(OutboxEvent::id).toList(), clock.instant());
            furthest = OutboxPosition.of(page.get(page.size() - 1));
            position = furthest;
            if (page.size() < BATCH) break;
        }
        if (furthest != null && saved.map(furthest::isAfter).orElse(true)) state.saveCursor(furthest);
    }

    private JobResult publishDue(String runId, Instant deadline) {
        var due = state.duePending(clock.instant(), BATCH);
        var events = source.findByIds(due.stream().map(PendingEvent::eventId).toList()).stream()
                .collect(Collectors.toMap(OutboxEvent::id, Function.identity()));
        int processed = 0, failed = 0;
        for (var pending : due) {
            if (!clock.instant().isBefore(deadline)) break;
            try { if (processTicketPdf(pending, events.get(pending.eventId()), runId)) processed++; else failed++; }
            catch (Exception e) { log.error("{} failed: {}", name, pending.eventId(), e); failed++; }
        }
        log.info("{} completed: ok={}, fail={}", name, processed, failed);
        return new JobResult(processed, failed);
    }

    private boolean processTicketPdf(PendingEvent pending, OutboxEvent event, String runId) {
        if (event == null) return fail(pending, null, "event missing", runId);
        var invalid = validator.validate(event);
        if (invalid.isPresent()) return fail(pending, event, invalid.get(), runId);

        var ticketData = parseTicketPdfData(event);
        byte[] pdfBytes;
        try { pdfBytes = pdfGenerator.generateTicketPdf(ticketData); }
        catch (Exception e) { log.error("PDF gen failed: {}", e.getMessage()); throw new RuntimeException(e); }

        String objectKey = "tickets/" + ticketData.reservationId() + "/" + ticketData.ticketId() + ".pdf";
        String pdfUrl;
        try { pdfUrl = objectStorage.uploadPdf(objectKey, pdfBytes, "application/pdf"); }
        catch (Exception e) { log.error("S3 upload failed: {}", e.getMessage()); throw new RuntimeException(e); }

        try { ticketingApi.submitPdfUrl(ticketData.ticketId().toString(), pdfUrl); }
        catch (Exception e) { log.error("API call failed: {}", e.getMessage()); throw new RuntimeException(e); }

        String correlationId = event.correlationId() != null ? event.correlationId() : runId;
        try { publisher.publish(event, correlationId); }
        catch (PublishException e) {
            if (e.isPermanent()) return fail(pending, event, e.getMessage(), runId);
            retryLater(pending, event, e, runId); return false;
        }
        state.markPublished(pending.eventId(), clock.instant());
        log.info("PDF generated: eventId={}, ticketId={}, pdfUrl={}", event.id(), ticketData.ticketId(), pdfUrl);
        return true;
    }

    private boolean fail(PendingEvent p, OutboxEvent e, String reason, String runId) {
        state.markFailed(p.eventId(), RelayStatus.FAILED_PERMANENT, p.attemptCount(), reason.substring(0, Math.min(reason.length(), 500)));
        log.error("{} failed permanently: eventId={}, reason={}", name, p.eventId(), reason);
        return false;
    }

    private void retryLater(PendingEvent p, OutboxEvent e, PublishException ex, String runId) {
        int a = p.attemptCount() + 1;
        if (retryPolicy.isExhausted(a)) { fail(p, e, "exhausted: " + ex.getMessage(), runId); return; }
        state.markRetry(p.eventId(), a, clock.instant().plus(retryPolicy.delay(a)), ex.getMessage());
    }

    private TicketPdfData parseTicketPdfData(OutboxEvent event) {
        // Simplified: extract from event.envelope() JSON
        // In production: use Jackson to parse event.envelope()
        return new TicketPdfData(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            "Movie", "Room", Instant.now(), List.of("A1","A2"), List.of(), "CSP-RES-A1-A2"
        );
    }

    @Override public String name() { return name; }
}
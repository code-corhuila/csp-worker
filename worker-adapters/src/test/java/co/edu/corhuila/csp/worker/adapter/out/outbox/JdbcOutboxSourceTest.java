package co.edu.corhuila.csp.worker.adapter.out.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.application.port.out.OutboxEvent;
import co.edu.corhuila.csp.worker.application.port.out.OutboxPosition;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;

/** The read-only view of {@code booking.outbox_event} against a real PostgreSQL. */
@EnabledIfEnvironmentVariable(named = "TEST_DATABASE_URL", matches = ".+")
class JdbcOutboxSourceTest {

    private static final UUID A = UUID.fromString("00000000-0000-4000-8000-00000000000a");
    private static final UUID B = UUID.fromString("00000000-0000-4000-8000-00000000000b");
    private static final UUID C = UUID.fromString("00000000-0000-4000-8000-00000000000c");

    private JdbcTemplate jdbc;
    private JdbcOutboxSource source;

    @BeforeEach
    void setUp() {
        jdbc = PostgresSupport.freshDatabase();
        source = new JdbcOutboxSource(jdbc);
    }

    @Test
    void theRowsComeOrderedByCreationAndThenById() {
        PostgresSupport.insertEvent(jdbc, C, "BookingConfirmed", "2026-10-07T12:00:02Z", "corr-c");
        PostgresSupport.insertEvent(jdbc, B, "ReservationHeld", "2026-10-07T12:00:01Z", "corr-b");
        PostgresSupport.insertEvent(jdbc, A, "ReservationHeld", "2026-10-07T12:00:01Z", "corr-a");

        List<OutboxEvent> rows = source.discover(OutboxPosition.START, 10);

        assertEquals(List.of(A, B, C), rows.stream().map(OutboxEvent::id).toList());
    }

    @Test
    void aRowCarriesItsEnvelopeItsTypeAndTheCorrelationIdOfTheEnvelope() {
        PostgresSupport.insertEvent(jdbc, A, "ReservationHeld", "2026-10-07T12:00:01Z", "corr-a");

        OutboxEvent row = source.discover(OutboxPosition.START, 10).get(0);

        assertEquals("ReservationHeld", row.eventType());
        assertEquals("corr-a", row.correlationId());
        assertEquals(Instant.parse("2026-10-07T12:00:01Z"), row.createdAt());
        assertTrue(row.envelope().contains("\"eventId\""));
    }

    @Test
    void onlyTheRowsAfterThePositionAreReturned() {
        PostgresSupport.insertEvent(jdbc, A, "ReservationHeld", "2026-10-07T12:00:01Z", "corr-a");
        PostgresSupport.insertEvent(jdbc, B, "ReservationHeld", "2026-10-07T12:00:01Z", "corr-b");
        PostgresSupport.insertEvent(jdbc, C, "BookingConfirmed", "2026-10-07T12:00:02Z", "corr-c");

        List<OutboxEvent> rows = source.discover(new OutboxPosition(Instant.parse("2026-10-07T12:00:01Z"), A), 10);

        assertEquals(List.of(B, C), rows.stream().map(OutboxEvent::id).toList());
    }

    @Test
    void theLimitBoundsThePage() {
        PostgresSupport.insertEvent(jdbc, A, "ReservationHeld", "2026-10-07T12:00:01Z", "corr-a");
        PostgresSupport.insertEvent(jdbc, B, "ReservationHeld", "2026-10-07T12:00:02Z", "corr-b");
        PostgresSupport.insertEvent(jdbc, C, "ReservationHeld", "2026-10-07T12:00:03Z", "corr-c");

        assertEquals(2, source.discover(OutboxPosition.START, 2).size());
    }

    @Test
    void anEventIsFoundByItsIdAndAMissingOneIsLeftOut() {
        PostgresSupport.insertEvent(jdbc, A, "ReservationHeld", "2026-10-07T12:00:01Z", "corr-a");

        List<OutboxEvent> rows = source.findByIds(List.of(A, B));

        assertEquals(List.of(A), rows.stream().map(OutboxEvent::id).toList());
    }

    @Test
    void noIdsFindNothing() {
        assertTrue(source.findByIds(List.of()).isEmpty());
    }
}

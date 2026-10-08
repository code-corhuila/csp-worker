package co.edu.corhuila.csp.worker.adapter.out.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.application.port.out.OutboxEvent;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** The envelope check of {@code events.md}: what a consumer needs to deduplicate and to trace. */
class EnvelopeSchemaValidatorTest {

    private static final UUID ID = UUID.fromString("11111111-1111-4111-8111-111111111111");
    private static final UUID AGGREGATE = UUID.fromString("22222222-2222-4222-8222-222222222222");

    private final EnvelopeSchemaValidator validator = new EnvelopeSchemaValidator();

    private static String envelope(String eventId, String eventType, String occurredAt, String version,
            String source, String aggregateId, String aggregateType, String payload, String metadata) {
        return "{\"eventId\":" + eventId + ",\"eventType\":" + eventType + ",\"occurredAt\":" + occurredAt
                + ",\"version\":" + version + ",\"source\":" + source + ",\"aggregateId\":" + aggregateId
                + ",\"aggregateType\":" + aggregateType + ",\"payload\":" + payload + ",\"metadata\":" + metadata + "}";
    }

    private static String valid() {
        return envelope("\"" + ID + "\"", "\"ReservationHeld\"", "\"2026-10-07T12:00:00Z\"", "1",
                "\"booking-service\"", "\"" + AGGREGATE + "\"", "\"Reservation\"", "{\"reservationId\":\"x\"}",
                "{\"correlationId\":\"c-1\",\"causationId\":null}");
    }

    private static OutboxEvent event(String envelope) {
        return new OutboxEvent(ID, "ReservationHeld", AGGREGATE, envelope, "c-1", Instant.parse("2026-10-07T12:00:00Z"));
    }

    @Test
    void aCompleteEnvelopeIsValid() {
        assertEquals(Optional.empty(), validator.validate(event(valid())));
    }

    @Test
    void textThatIsNotJsonIsInvalid() {
        assertTrue(validator.validate(event("not json")).isPresent());
    }

    @Test
    void aMissingFieldIsNamedInTheReason() {
        String withoutPayload = valid().replace(",\"payload\":{\"reservationId\":\"x\"}", "");

        assertEquals(Optional.of("envelope.payload must be an object"), validator.validate(event(withoutPayload)));
    }

    @Test
    void theEnvelopeMustBelongToTheRowItWasReadFrom() {
        String otherId = valid().replace(ID.toString(), "33333333-3333-4333-8333-333333333333");

        assertEquals(Optional.of("envelope.eventId must equal the id of the outbox row"),
                validator.validate(event(otherId)));
    }

    @Test
    void theTypeAndTheAggregateMustMatchTheRow() {
        String otherType = valid().replace("\"ReservationHeld\"", "\"BookingConfirmed\"");
        String otherAggregate = valid().replace(AGGREGATE.toString(), "44444444-4444-4444-8444-444444444444");

        assertEquals(Optional.of("envelope.eventType must equal the type of the outbox row"),
                validator.validate(event(otherType)));
        assertEquals(Optional.of("envelope.aggregateId must equal the aggregate of the outbox row"),
                validator.validate(event(otherAggregate)));
    }

    @Test
    void theSourceMustBeTheBookingService() {
        String other = valid().replace("booking-service", "catalog-service");

        assertEquals(Optional.of("envelope.source must be booking-service"), validator.validate(event(other)));
    }

    @Test
    void theVersionIsAPositiveInteger() {
        assertEquals(Optional.of("envelope.version must be a positive integer"),
                validator.validate(event(valid().replace("\"version\":1", "\"version\":0"))));
        assertEquals(Optional.of("envelope.version must be a positive integer"),
                validator.validate(event(valid().replace("\"version\":1", "\"version\":\"1\""))));
    }

    @Test
    void occurredAtIsAnInstant() {
        assertEquals(Optional.of("envelope.occurredAt must be an RFC 3339 instant"),
                validator.validate(event(valid().replace("2026-10-07T12:00:00Z", "yesterday"))));
    }

    @Test
    void theCorrelationIdOfTheMetadataIsRequired() {
        String without = valid().replace("\"correlationId\":\"c-1\",", "");

        assertEquals(Optional.of("envelope.metadata.correlationId must be a string"),
                validator.validate(event(without)));
    }
}

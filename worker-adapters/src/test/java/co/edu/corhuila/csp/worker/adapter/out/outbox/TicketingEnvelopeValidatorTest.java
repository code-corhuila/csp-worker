package co.edu.corhuila.csp.worker.adapter.out.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;

import co.edu.corhuila.csp.worker.application.port.out.OutboxEvent;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Contract of the TicketIssued envelope as {@code csp-ticketing-api} writes it (ADR-057): a traced envelope. */
class TicketingEnvelopeValidatorTest {

    private static final UUID ID = UUID.fromString("11111111-1111-4111-8111-111111111111");
    private static final UUID TICKET = UUID.fromString("22222222-2222-4222-8222-222222222222");

    private final EnvelopeSchemaValidator validator = new EnvelopeSchemaValidator(RelayDomain.TICKETING);

    private static String envelope(String source, String aggregateType, String metadata) {
        return "{\"eventId\":\"" + ID + "\",\"eventType\":\"TicketIssued\",\"occurredAt\":\"2026-11-01T15:00:00Z\","
                + "\"version\":1,\"source\":\"" + source + "\",\"aggregateId\":\"" + TICKET + "\","
                + "\"aggregateType\":" + aggregateType + ",\"payload\":{\"ticketId\":\"" + TICKET + "\"},\"metadata\":"
                + metadata + "}";
    }

    private Optional<String> validate(String envelope) {
        return validator.validate(new OutboxEvent(ID, "TicketIssued", TICKET, envelope, null,
                Instant.parse("2026-11-01T15:00:00Z")));
    }

    @Test
    void theEnvelopeTheApiWritesIsValid() {
        assertEquals(Optional.empty(),
                validate(envelope("ticketing-fulfillment-service", "\"Ticket\"", "{\"correlationId\":\"corr-1\"}")));
    }

    @Test
    void anEnvelopeOfAnotherServiceIsInvalid() {
        assertEquals(Optional.of("envelope.source must be ticketing-fulfillment-service"),
                validate(envelope("booking-service", "\"Ticket\"", "{\"correlationId\":\"c\"}")));
    }

    @Test
    void theAggregateTypeAndTheCorrelationIdAreRequired() {
        assertEquals(Optional.of("envelope.aggregateType must be a string"),
                validate(envelope("ticketing-fulfillment-service", "null", "{\"correlationId\":\"c\"}")));
        assertEquals(Optional.of("envelope.metadata.correlationId must be a string"),
                validate(envelope("ticketing-fulfillment-service", "\"Ticket\"", "{}")));
    }
}

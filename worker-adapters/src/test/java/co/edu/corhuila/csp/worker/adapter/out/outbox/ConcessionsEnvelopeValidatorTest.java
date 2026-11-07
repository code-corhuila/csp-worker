package co.edu.corhuila.csp.worker.adapter.out.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;

import co.edu.corhuila.csp.worker.application.port.out.OutboxEvent;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Contract of the Concessions envelope as {@code csp-concessions-api} writes it (ADR-045): a traced envelope. */
class ConcessionsEnvelopeValidatorTest {

    private static final UUID ID = UUID.fromString("11111111-1111-4111-8111-111111111111");
    private static final UUID ORDER = UUID.fromString("22222222-2222-4222-8222-222222222222");

    private final EnvelopeSchemaValidator validator = new EnvelopeSchemaValidator(RelayDomain.CONCESSIONS);

    private static String envelope(String eventType, String source, String aggregateType, String metadata) {
        return "{\"eventId\":\"" + ID + "\",\"eventType\":\"" + eventType + "\",\"occurredAt\":\"2026-10-22T15:00:00Z\","
                + "\"version\":1,\"source\":\"" + source + "\",\"aggregateId\":\"" + ORDER + "\","
                + "\"aggregateType\":" + aggregateType + ",\"payload\":{\"orderId\":\"" + ORDER + "\"},\"metadata\":" + metadata + "}";
    }

    private Optional<String> validate(String eventType, String envelope) {
        return validator.validate(new OutboxEvent(ID, eventType, ORDER, envelope, null, Instant.parse("2026-10-22T15:00:00Z")));
    }

    @Test
    void theEnvelopeTheApiWritesIsValidForEveryEventOfEventsMd() {
        for (String type : new String[] {"ConcessionOrderCreated", "ConcessionStockReserved", "ConcessionOrderConfirmed",
                "ConcessionOrderCancelled"}) {
            assertEquals(Optional.empty(), validate(type,
                    envelope(type, "concessions-service", "\"ConcessionOrder\"", "{\"correlationId\":\"corr-1\"}")), type);
        }
    }

    @Test
    void anEnvelopeOfAnotherServiceIsInvalid() {
        assertEquals(Optional.of("envelope.source must be concessions-service"), validate("ConcessionOrderConfirmed",
                envelope("ConcessionOrderConfirmed", "booking-service", "\"ConcessionOrder\"", "{\"correlationId\":\"c\"}")));
    }

    @Test
    void theAggregateTypeAndTheCorrelationIdAreRequired() {
        assertEquals(Optional.of("envelope.aggregateType must be a string"), validate("ConcessionOrderConfirmed",
                envelope("ConcessionOrderConfirmed", "concessions-service", "null", "{\"correlationId\":\"c\"}")));
        assertEquals(Optional.of("envelope.metadata.correlationId must be a string"), validate("ConcessionOrderConfirmed",
                envelope("ConcessionOrderConfirmed", "concessions-service", "\"ConcessionOrder\"", "{}")));
    }
}

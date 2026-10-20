package co.edu.corhuila.csp.worker.adapter.out.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;

import co.edu.corhuila.csp.worker.application.port.out.OutboxEvent;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Contract of the auth envelope: the three examples of {@code events.md} of the auth service, as
 * {@code csp-auth-api} writes them (no {@code aggregateType} and no {@code metadata} inside).
 */
class AuthEnvelopeValidatorTest {

    private static final UUID ID = UUID.fromString("11111111-1111-4111-8111-111111111111");
    private static final UUID USER = UUID.fromString("22222222-2222-4222-8222-222222222222");

    private final EnvelopeSchemaValidator validator = new EnvelopeSchemaValidator(RelayDomain.AUTH);

    private static String envelope(String eventType, String source, String payload) {
        return "{\"eventId\":\"" + ID + "\",\"eventType\":\"" + eventType + "\",\"occurredAt\":\"2026-10-16T15:00:00Z\","
                + "\"version\":1,\"source\":\"" + source + "\",\"aggregateId\":\"" + USER + "\",\"payload\":" + payload + "}";
    }

    private Optional<String> validate(String eventType, String envelope) {
        return validator.validate(new OutboxEvent(ID, eventType, USER, envelope, null, Instant.parse("2026-10-16T15:00:00Z")));
    }

    @Test
    void theThreeEventsOfEventsMdAreValid() {
        assertEquals(Optional.empty(), validate("UserRegistered", envelope("UserRegistered", "auth-service",
                "{\"userId\":\"" + USER + "\",\"email\":\"user@example.com\",\"roles\":[\"CLIENT\"]}")));
        assertEquals(Optional.empty(), validate("UserEmailChanged", envelope("UserEmailChanged", "auth-service",
                "{\"userId\":\"" + USER + "\",\"email\":\"new@example.com\"}")));
        assertEquals(Optional.empty(), validate("UserRoleChanged", envelope("UserRoleChanged", "auth-service",
                "{\"userId\":\"" + USER + "\",\"role\":\"ADMIN\",\"enabled\":true,\"changedBy\":\"" + UUID.randomUUID() + "\"}")));
    }

    @Test
    void anEnvelopeOfAnotherServiceIsInvalid() {
        assertEquals(Optional.of("envelope.source must be auth-service"),
                validate("UserRegistered", envelope("UserRegistered", "booking-service", "{}")));
    }

    @Test
    void theEnvelopeMustBelongToTheRowItWasReadFrom() {
        assertEquals(Optional.of("envelope.eventType must equal the type of the outbox row"),
                validate("UserEmailChanged", envelope("UserRegistered", "auth-service", "{}")));
    }

    @Test
    void thePayloadIsStillRequired() {
        assertEquals(Optional.of("envelope.payload must be an object"),
                validate("UserRegistered", envelope("UserRegistered", "auth-service", "null")));
    }
}

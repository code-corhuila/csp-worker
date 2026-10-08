package co.edu.corhuila.csp.worker.adapter.out.outbox;

import co.edu.corhuila.csp.worker.application.port.out.EnvelopeValidator;
import co.edu.corhuila.csp.worker.application.port.out.OutboxEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.format.DateTimeParseException;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Checks the event envelope of {@code events.md} before it is published: the fields a consumer
 * needs to deduplicate by {@code eventId} and to trace by {@code correlationId}, and that the
 * envelope is the one of the outbox row it came from. An invalid envelope is never retried, so the
 * reason names the first field that fails and carries no data of the payload.
 */
public class EnvelopeSchemaValidator implements EnvelopeValidator {

    private static final String SOURCE = "booking-service";

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Optional<String> validate(OutboxEvent event) {
        JsonNode envelope;
        try {
            envelope = mapper.readTree(event.envelope());
        } catch (JsonProcessingException exception) {
            return Optional.of("envelope is not valid JSON");
        }
        if (envelope == null || !envelope.isObject()) {
            return Optional.of("envelope must be a JSON object");
        }
        if (!hasUuid(envelope, "eventId", event.id())) {
            return Optional.of("envelope.eventId must equal the id of the outbox row");
        }
        if (!event.eventType().equals(text(envelope, "eventType"))) {
            return Optional.of("envelope.eventType must equal the type of the outbox row");
        }
        if (!isInstant(text(envelope, "occurredAt"))) {
            return Optional.of("envelope.occurredAt must be an RFC 3339 instant");
        }
        if (!envelope.path("version").isInt() || envelope.path("version").asInt() < 1) {
            return Optional.of("envelope.version must be a positive integer");
        }
        if (!SOURCE.equals(text(envelope, "source"))) {
            return Optional.of("envelope.source must be " + SOURCE);
        }
        if (!hasUuid(envelope, "aggregateId", event.aggregateId())) {
            return Optional.of("envelope.aggregateId must equal the aggregate of the outbox row");
        }
        if (text(envelope, "aggregateType") == null || text(envelope, "aggregateType").isBlank()) {
            return Optional.of("envelope.aggregateType must be a string");
        }
        if (!envelope.path("payload").isObject()) {
            return Optional.of("envelope.payload must be an object");
        }
        JsonNode metadata = envelope.path("metadata");
        if (!metadata.isObject()) {
            return Optional.of("envelope.metadata must be an object");
        }
        if (text(metadata, "correlationId") == null) {
            return Optional.of("envelope.metadata.correlationId must be a string");
        }
        return Optional.empty();
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isTextual() ? value.asText() : null;
    }

    private static boolean hasUuid(JsonNode node, String field, UUID expected) {
        String value = text(node, field);
        return value != null && value.equals(expected.toString());
    }

    private static boolean isInstant(String value) {
        if (value == null) {
            return false;
        }
        try {
            Instant.parse(value);
            return true;
        } catch (DateTimeParseException exception) {
            return false;
        }
    }
}

package co.edu.corhuila.csp.worker.application.port.out;

import java.util.Optional;

/** Checks the envelope of an event against its schema before it is published. */
public interface EnvelopeValidator {

    /** The reason the envelope is invalid, empty when it is valid. */
    Optional<String> validate(OutboxEvent event);
}

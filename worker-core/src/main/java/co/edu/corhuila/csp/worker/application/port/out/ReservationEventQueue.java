package co.edu.corhuila.csp.worker.application.port.out;

import java.util.Optional;

/**
 * The queues of Concessions where the Booking events wait (ADR-036, ADR-045), as the worker sees them:
 * one message at a time, taken on request so a run stays bounded (Norma 5.7). A message leaves the queue only
 * when the job acknowledges it.
 */
public interface ReservationEventQueue {

    /** The next waiting message of any queue, or empty when there is none. */
    Optional<Delivery> next();

    /** One message taken from a queue and not yet settled. */
    interface Delivery {

        /** The event it carries, or empty when the message cannot be read as one of the two events. */
        Optional<ReservationEvent> event();

        /** The message was handled: it leaves the queue. */
        void ack();

        /** The message can never be handled: it goes to the dead-letter queue (ADR-007). */
        void reject();

        /** The message could not be handled now: it goes back to the queue for a later run. */
        void requeue();
    }
}

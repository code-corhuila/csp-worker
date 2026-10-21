package co.edu.corhuila.csp.worker.application.port.out;

import java.util.Optional;

/**
 * Mutual exclusion of the outbox relay between worker instances: ADR-014 runs one relay per
 * environment, and this lock makes a second instance step aside instead of publishing the same
 * events twice.
 */
public interface RunLock {

    /**
     * Takes the lock if no other instance holds it.
     *
     * @return the lease while this instance is the only one running, empty when another holds it
     */
    Optional<Lease> tryAcquire();

    /** The lock while it is held. Closing it gives the lock back; it never throws. */
    interface Lease extends AutoCloseable {

        @Override
        void close();
    }
}

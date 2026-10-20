package co.edu.corhuila.csp.worker.adapter.out.outbox;

import co.edu.corhuila.csp.worker.application.port.out.RunLock;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * The instance lock of the relay of one domain as a PostgreSQL session advisory lock. The lock belongs to the
 * session, so it is held on a connection of its own for as long as the run lasts; closing that
 * connection ends the session and PostgreSQL gives the lock back, and it does the same if the
 * process or the connection dies, so a crashed instance never blocks the others for good. The
 * data source must not pool: a pooled connection would stay open, with the lock, after the run.
 */
public class JdbcAdvisoryRunLock implements RunLock {

    private final DataSource dataSource;
    private final long key;

    /** One fixed key per domain, so the relays of two domains in the same instance do not wait for each other. */
    public JdbcAdvisoryRunLock(DataSource dataSource, RelayDomain domain) {
        this.dataSource = dataSource;
        this.key = domain.lockKey();
    }

    @Override
    public Optional<Lease> tryAcquire() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            if (granted(connection, key)) {
                return Optional.of(new Held(connection));
            }
            connection.close();
            return Optional.empty();
        } catch (SQLException exception) {
            closeQuietly(connection);
            throw new IllegalStateException("cannot ask PostgreSQL for the lock of the outbox relay", exception);
        }
    }

    private static boolean granted(Connection connection, long key) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
            statement.setLong(1, key);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() && result.getBoolean(1);
            }
        }
    }

    private static void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
                // The session is gone or going; PostgreSQL releases whatever it held.
            }
        }
    }

    private record Held(Connection connection) implements Lease {

        @Override
        public void close() {
            closeQuietly(connection);
        }
    }
}

package co.edu.corhuila.csp.worker.adapter.out.outbox;

import co.edu.corhuila.csp.worker.application.port.out.DependencyProbe;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Asks PostgreSQL whether it answers, with a connection of its own and short timeouts. It does not borrow one
 * from the pool of the relay: a pool that waits for a connection would keep the readiness request waiting too.
 */
public class JdbcDependencyProbe implements DependencyProbe {

    private static final Logger log = LoggerFactory.getLogger(JdbcDependencyProbe.class);
    private static final int TIMEOUT_SECONDS = 2;

    private final String url;
    private final Properties credentials = new Properties();

    public JdbcDependencyProbe(String url, String username, String password) {
        this.url = url;
        credentials.setProperty("user", username == null ? "" : username);
        credentials.setProperty("password", password == null ? "" : password);
        credentials.setProperty("loginTimeout", String.valueOf(TIMEOUT_SECONDS));
        credentials.setProperty("connectTimeout", String.valueOf(TIMEOUT_SECONDS));
        credentials.setProperty("socketTimeout", String.valueOf(TIMEOUT_SECONDS));
    }

    @Override
    public String dependency() {
        return "postgresql";
    }

    @Override
    public boolean isReachable() {
        try (Connection connection = DriverManager.getConnection(url, credentials)) {
            return connection.isValid(TIMEOUT_SECONDS);
        } catch (SQLException | RuntimeException unreachable) {
            log.warn("readiness: postgresql does not answer: {}", unreachable.getClass().getSimpleName());
            return false;
        }
    }
}

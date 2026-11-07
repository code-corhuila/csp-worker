package co.edu.corhuila.csp.worker.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariDataSource;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Every outbox relay declares its own {@link HikariDataSource}, and its job receives it by type. With two relays on, Spring
 * tells the beans apart by the name of the parameter, so the classes must be compiled with {@code -parameters}.
 */
class RelayDataSourceBindingTest {

    @Test
    void theJobOfARelayAsksForItsDataSourceByName() {
        Parameter dataSource = firstParameterOf(ConcessionsOutboxRelayConfiguration.class, "concessionsOutboxRelayJob");

        assertTrue(dataSource.isNamePresent(), "the classes are not compiled with -parameters");
        assertEquals("concessionsRelayDataSource", dataSource.getName());
    }

    private static Parameter firstParameterOf(Class<?> type, String method) {
        Method found = Arrays.stream(type.getDeclaredMethods()).filter(candidate -> candidate.getName().equals(method)).findFirst()
                .orElseThrow();
        return found.getParameters()[0];
    }
}

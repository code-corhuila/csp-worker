package co.edu.corhuila.csp.worker.app;

import co.edu.corhuila.csp.worker.adapter.out.amqp.AmqpEventPublisher;
import co.edu.corhuila.csp.worker.adapter.out.amqp.AmqpReservationEvents;
import co.edu.corhuila.csp.worker.adapter.out.outbox.JdbcDependencyProbe;
import co.edu.corhuila.csp.worker.application.port.out.DependencyProbe;
import co.edu.corhuila.csp.worker.application.usecase.ReadinessCheck;
import com.zaxxer.hikari.HikariDataSource;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The readiness of the worker (ADR-014): one probe for the database and one for the broker of every relay that was
 * composed. It asks the beans that exist instead of the flags, so a relay that is off adds no dependency and a new
 * relay is probed without touching this class.
 */
@Configuration
public class ReadinessConfiguration {

    @Bean
    ReadinessCheck readinessCheck(ObjectProvider<HikariDataSource> databases,
            ObjectProvider<AmqpEventPublisher> publishers, ObjectProvider<AmqpReservationEvents> queues) {
        List<DependencyProbe> probes = new ArrayList<>();
        databases.orderedStream().forEach(database ->
                probes.add(new JdbcDependencyProbe(database.getJdbcUrl(), database.getUsername(), database.getPassword())));
        publishers.orderedStream().forEach(publisher -> probes.add(publisher.brokerProbe()));
        queues.orderedStream().forEach(queue -> probes.add(queue.brokerProbe()));
        return new ReadinessCheck(probes);
    }
}

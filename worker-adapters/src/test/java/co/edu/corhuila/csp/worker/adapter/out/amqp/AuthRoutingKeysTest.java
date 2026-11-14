package co.edu.corhuila.csp.worker.adapter.out.amqp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import co.edu.corhuila.csp.worker.adapter.out.outbox.RelayDomain;
import org.junit.jupiter.api.Test;

/** Contract of the routing keys of the auth service in its {@code events.md}. */
class AuthRoutingKeysTest {

    @Test
    void everyAuthEventIsRoutedWithTheKeyOfEventsMd() {
        assertEquals("cine.auth.user-registered.v1", AmqpEventPublisher.routingKeyOf(RelayDomain.AUTH, "UserRegistered", 1));
        assertEquals("cine.auth.user-email-changed.v1",
                AmqpEventPublisher.routingKeyOf(RelayDomain.AUTH, "UserEmailChanged", 1));
        assertEquals("cine.auth.user-role-changed.v1",
                AmqpEventPublisher.routingKeyOf(RelayDomain.AUTH, "UserRoleChanged", 1));
    }
}

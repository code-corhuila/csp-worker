package co.edu.corhuila.csp.worker.adapter.out.amqp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import co.edu.corhuila.csp.worker.adapter.out.outbox.RelayDomain;
import org.junit.jupiter.api.Test;

/** Contract of the routing key of Ticketing in its {@code events.md}: {@code cine.ticketing_fulfillment.ticket-issued.v1}. */
class TicketingRoutingKeysTest {

    @Test
    void ticketIssuedIsRoutedWithTheKeyOfEventsMd() {
        assertEquals("cine.ticketing_fulfillment.ticket-issued.v1",
                AmqpEventPublisher.routingKeyOf(RelayDomain.TICKETING, "TicketIssued", 1));
    }
}

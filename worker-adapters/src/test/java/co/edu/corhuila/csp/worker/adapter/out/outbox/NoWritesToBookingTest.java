package co.edu.corhuila.csp.worker.adapter.out.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Criterion 3 of ADR-014: the worker never executes {@code INSERT}, {@code UPDATE}, {@code DELETE}
 * or {@code TRUNCATE} against schema {@code booking}, and the same holds for {@code auth} (ADR-019). It reads
 * the outbox and nothing else, and the grants of the reader roles would refuse the rest anyway.
 */
class NoWritesToBookingTest {

    private static final Pattern WRITE_ON_BOOKING = Pattern.compile(
            "(insert\\s+into|update|delete\\s+from|truncate(\\s+table)?)\\s+(booking|auth)\\.", Pattern.CASE_INSENSITIVE);

    @Test
    void noStatementOfTheWorkerWritesTheBookingOrAuthSchema() throws IOException {
        List<Path> sources;
        try (Stream<Path> files = Files.walk(Path.of("..")).filter(path -> path.toString().endsWith(".java")
                && path.toString().contains("src" + java.io.File.separator + "main"))) {
            sources = files.toList();
        }

        assertFalse(sources.isEmpty(), "the scan must find the sources of the worker");
        for (Path source : sources) {
            assertEquals(false, WRITE_ON_BOOKING.matcher(Files.readString(source)).find(),
                    source + " writes to the booking or auth schema");
        }
    }

    @Test
    void theScanRecognizesAWriteWhenThereIsOne() {
        assertEquals(true, WRITE_ON_BOOKING.matcher("UPDATE booking.outbox_event SET x = 1").find());
        assertEquals(true, WRITE_ON_BOOKING.matcher("DELETE FROM auth.outbox_event").find());
        assertEquals(false, WRITE_ON_BOOKING.matcher("SELECT id FROM booking.outbox_event").find());
        assertEquals(false, WRITE_ON_BOOKING.matcher("SELECT id FROM auth.outbox_event").find());
    }
}

package co.edu.corhuila.csp.worker.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.PropertySourcesPropertyResolver;
import org.springframework.core.io.ClassPathResource;

/**
 * The platform shares one env file between services, and there {@code OBJECT_STORAGE_*} is the read-only account of
 * {@code csp-ticketing-api}. The worker writes with another account, so it reads its own variables, {@code PDF_STORAGE_*}.
 */
class PdfStorageVariablesTest {

    @Test
    void theWriterSettingsComeFromThePdfStorageVariables() throws IOException {
        PropertySourcesPropertyResolver settings = resolverWith(Map.of(
                "PDF_STORAGE_URL", "http://storage:9000",
                "PDF_STORAGE_ACCESS_KEY", "ticketing_pdf_writer",
                "PDF_STORAGE_SECRET_KEY", "writer-secret",
                "PDF_STORAGE_BUCKET", "pdfs",
                "PDF_STORAGE_REGION", "eu-west-1"));

        assertEquals("http://storage:9000", settings.getProperty("object-storage.url"));
        assertEquals("ticketing_pdf_writer", settings.getProperty("object-storage.access-key"));
        assertEquals("writer-secret", settings.getProperty("object-storage.secret-key"));
        assertEquals("pdfs", settings.getProperty("object-storage.bucket"));
        assertEquals("eu-west-1", settings.getProperty("object-storage.region"));
    }

    @Test
    void theDefaultsPointAtTheTicketsBucketOfThePlatformStorageWithoutCredentials() throws IOException {
        PropertySourcesPropertyResolver settings = resolverWith(Map.of());

        assertEquals("http://rustfs:9000", settings.getProperty("object-storage.url"));
        assertEquals("", settings.getProperty("object-storage.access-key"));
        assertEquals("", settings.getProperty("object-storage.secret-key"));
        assertEquals("tickets", settings.getProperty("object-storage.bucket"));
        assertEquals("us-east-1", settings.getProperty("object-storage.region"));
    }

    @Test
    void theReaderAccountOfTheSharedEnvFileIsNeverTaken() throws IOException {
        PropertySourcesPropertyResolver settings = resolverWith(Map.of(
                "OBJECT_STORAGE_URL", "http://reader-storage:9000",
                "OBJECT_STORAGE_ACCESS_KEY", "ticketing_pdf_reader",
                "OBJECT_STORAGE_SECRET_KEY", "reader-secret",
                "OBJECT_STORAGE_BUCKET", "other"));

        assertEquals("http://rustfs:9000", settings.getProperty("object-storage.url"));
        assertEquals("", settings.getProperty("object-storage.access-key"));
        assertEquals("", settings.getProperty("object-storage.secret-key"));
        assertEquals("tickets", settings.getProperty("object-storage.bucket"));
    }

    private static PropertySourcesPropertyResolver resolverWith(Map<String, Object> environment) throws IOException {
        MutablePropertySources sources = new MutablePropertySources();
        sources.addLast(new MapPropertySource("environment", environment));
        for (PropertySource<?> source : new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml"))) {
            sources.addLast(source);
        }
        return new PropertySourcesPropertyResolver(sources);
    }
}

package co.edu.corhuila.csp.worker.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The csp-worker application: runs scheduled background jobs (Norma 4.3). It exposes no business
 * interface, only a health endpoint (Norma 5.7.1).
 */
@SpringBootApplication
public class WorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }
}
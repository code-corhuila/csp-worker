package co.edu.corhuila.csp.worker.adapter.in.http;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import co.edu.corhuila.csp.worker.application.port.out.DependencyProbe;
import co.edu.corhuila.csp.worker.application.usecase.ReadinessCheck;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class HealthControllerTest {

    private static DependencyProbe probe(String dependency, boolean reachable) {
        return new DependencyProbe() {
            @Override
            public String dependency() {
                return dependency;
            }

            @Override
            public boolean isReachable() {
                return reachable;
            }
        };
    }

    private static MockMvc mvc(DependencyProbe... probes) {
        return MockMvcBuilders.standaloneSetup(new HealthController(new ReadinessCheck(List.of(probes)))).build();
    }

    @Test
    void healthSaysUpWithoutAskingAnyDependency() throws Exception {
        DependencyProbe broken = new DependencyProbe() {
            @Override
            public String dependency() {
                return "postgresql";
            }

            @Override
            public boolean isReachable() {
                throw new AssertionError("/health must not ask a dependency");
            }
        };

        mvc(broken).perform(get("/health"))
                .andExpect(status().isOk())
                .andExpect(header().string("Cache-Control", "no-store"))
                .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    void readyIsUpWhenEveryDependencyAnswers() throws Exception {
        mvc(probe("postgresql", true), probe("rabbitmq", true)).perform(get("/health/ready"))
                .andExpect(status().isOk())
                .andExpect(header().string("Cache-Control", "no-store"))
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.dependencies.postgresql").value("UP"))
                .andExpect(jsonPath("$.dependencies.rabbitmq").value("UP"));
    }

    @Test
    void readyAnswers503WhenOneDependencyDoesNotAnswer() throws Exception {
        mvc(probe("postgresql", true), probe("rabbitmq", false)).perform(get("/health/ready"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value("DOWN"))
                .andExpect(jsonPath("$.dependencies.postgresql").value("UP"))
                .andExpect(jsonPath("$.dependencies.rabbitmq").value("DOWN"));
    }

    @Test
    void aWorkerWithNoRelayIsReady() throws Exception {
        mvc().perform(get("/health/ready"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.dependencies").isEmpty());
    }

    @Test
    void theHealthPointIsTheOnlyHttpInterface() throws Exception {
        mvc(probe("postgresql", true)).perform(get("/metrics")).andExpect(status().isNotFound());
        mvc(probe("postgresql", true)).perform(get("/jobs")).andExpect(status().isNotFound());
    }
}

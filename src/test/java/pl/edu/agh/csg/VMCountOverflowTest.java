package pl.edu.agh.csg;

import com.google.gson.Gson;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static org.junit.Assert.assertTrue;

public class VMCountOverflowTest {

    final MultiSimulationEnvironment multiSimulationEnvironment = new MultiSimulationEnvironment();
    final Gson gson = new Gson();

    @Test
    public void testHandleNegativeMi() throws Exception {
        withEnvironmentVariable("DATACENTER_HOSTS_CNT", "5").execute(() -> {
            System.out.println("DATACENTER_HOSTS_CNT: " + System.getenv("DATACENTER_HOSTS_CNT"));

            CloudletDescriptor cloudletDescriptor = new CloudletDescriptor(1, 60, -778, 1);

            List<CloudletDescriptor> jobs = Arrays.asList(cloudletDescriptor);
            Map<String, String> parameters = new HashMap<>();
            parameters.put(SimulationFactory.SOURCE_OF_JOBS_PARAMS_JOBS, gson.toJson(jobs));
            parameters.put(SimulationFactory.SIMULATION_SPEEDUP, "60.0");
            parameters.put(SimulationFactory.SPLIT_LARGE_JOBS, "true");
            parameters.put(SimulationFactory.QUEUE_WAIT_PENALTY, "0.00001");
            parameters.put(SimulationFactory.INITIAL_L_VM_COUNT, "1");
            parameters.put(SimulationFactory.INITIAL_M_VM_COUNT, "1");
            parameters.put(SimulationFactory.INITIAL_S_VM_COUNT, "1");

            final String simulationId = multiSimulationEnvironment.createSimulation(parameters);

            multiSimulationEnvironment.reset(simulationId);

            int i = 0;
            while (i++ < 1000) {
                SimulationStepResult result = multiSimulationEnvironment.step(simulationId, 1);
                System.out.println("Result: " + result);

                if(result.isDone()) {
                    break;
                }
            }

            multiSimulationEnvironment.close(simulationId);
            assertTrue("There should be much less than a 1000 iterations!", i < 1000);
        });
    }
}

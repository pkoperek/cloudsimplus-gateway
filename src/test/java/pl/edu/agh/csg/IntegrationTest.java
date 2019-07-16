package pl.edu.agh.csg;

import static org.junit.Assert.*;

import com.google.gson.Gson;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.junit.Test;

import java.util.*;

public class IntegrationTest {

    final MultiSimulationEnvironment multiSimulationEnvironment = new MultiSimulationEnvironment();
    final Gson gson = new Gson();

    @Test
    public void testPing() {
        final long ping = multiSimulationEnvironment.ping();

        assertEquals(31415L, ping);
    }

    @Test
    public void testSimulationSingleStep() {
        CloudletDescriptor cloudletDescriptor = new CloudletDescriptor(1, 10, 10000, 4);

        List<CloudletDescriptor> jobs = Arrays.asList(cloudletDescriptor);
        Map<String, String> parameters = new HashMap<>();
        parameters.put(SimulationFactory.SOURCE_OF_JOBS_PARAMS_JOBS, gson.toJson(jobs));

        final String simulationId = multiSimulationEnvironment.createSimulation(parameters);

        multiSimulationEnvironment.reset(simulationId);
        multiSimulationEnvironment.step(simulationId, 0);
        multiSimulationEnvironment.close(simulationId);
    }

    @Test
    public void testSimulationWithSingleJob() {
        // Job should start after 10 iterations and last for next 10
        CloudletDescriptor cloudletDescriptor = new CloudletDescriptor(1, 10, 100000, 4);

        List<CloudletDescriptor> jobs = Arrays.asList(cloudletDescriptor);
        Map<String, String> parameters = new HashMap<>();
        parameters.put(SimulationFactory.SOURCE_OF_JOBS_PARAMS_JOBS, gson.toJson(jobs));

        final String simulationId = multiSimulationEnvironment.createSimulation(parameters);

        multiSimulationEnvironment.reset(simulationId);
        int stepsExecuted = 1;
        SimulationStepResult step = multiSimulationEnvironment.step(simulationId, 0);
        while (!step.isDone()) {
            System.out.println("Executing step: " + stepsExecuted);
            step = multiSimulationEnvironment.step(simulationId, 0);
            stepsExecuted++;
        }
        multiSimulationEnvironment.close(simulationId);

        assertEquals(20, stepsExecuted);
    }

    @Test
    public void testWithCreatingNewVirtualMachines() {
        // every cloudlet executes for 40 simulation iterations and starts with a delay of 20*i iterations
        List<CloudletDescriptor> jobs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            jobs.add(new CloudletDescriptor(i, 20 * i, 400000, 4));
        }

        Map<String, String> parameters = new HashMap<>();
        parameters.put(SimulationFactory.INITIAL_VM_COUNT, "1");
        parameters.put(SimulationFactory.SOURCE_OF_JOBS_PARAMS_JOBS, gson.toJson(jobs));

        final String simulationId = multiSimulationEnvironment.createSimulation(parameters);

        multiSimulationEnvironment.reset(simulationId);
        int stepsExecuted = 1;
        SimulationStepResult step = multiSimulationEnvironment.step(simulationId, 0);

        double maxVmsCnt = 0.0;
        while (!step.isDone()) {
            System.out.println("Executing step: " + stepsExecuted);

            int action = stepsExecuted == 20 ? 1 : 0;

            step = multiSimulationEnvironment.step(simulationId, action);
            if (step.getObs()[0] > maxVmsCnt) {
                maxVmsCnt = step.getObs()[0];
            }

            System.out.println("Observations: " + Arrays.toString(step.getObs()) + " clock: " + multiSimulationEnvironment.clock(simulationId));
            stepsExecuted++;
        }
        multiSimulationEnvironment.close(simulationId);

        assertEquals(2, maxVmsCnt, 0.000001);
    }

    @Test
    public void testWithDestroyingVMs() {
        List<CloudletDescriptor> jobs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            jobs.add(new CloudletDescriptor(i, 10 * i, 200000, 4));
        }

        Map<String, String> parameters = new HashMap<>();
        parameters.put(SimulationFactory.INITIAL_VM_COUNT, "10");
        parameters.put(SimulationFactory.SOURCE_OF_JOBS_PARAMS_JOBS, gson.toJson(jobs));

        final String simulationId = multiSimulationEnvironment.createSimulation(parameters);

        multiSimulationEnvironment.reset(simulationId);
        int stepsExecuted = 1;
        SimulationStepResult step = multiSimulationEnvironment.step(simulationId, 0);

        while (!step.isDone()) {
            System.out.println("Executing step: " + stepsExecuted);

            if (stepsExecuted == 20) {
                step = multiSimulationEnvironment.step(simulationId, 2);
                assertEquals(9.0, step.getObs()[0], 0.1);
            } else {
                step = multiSimulationEnvironment.step(simulationId, 0);
            }

            System.out.println("Observations: " + Arrays.toString(step.getObs()) + " " + multiSimulationEnvironment.clock(simulationId));
            stepsExecuted++;
        }
        multiSimulationEnvironment.close(simulationId);
    }

    @Test
    public void testOfMetrics() {
        fail("implement me");
    }
}

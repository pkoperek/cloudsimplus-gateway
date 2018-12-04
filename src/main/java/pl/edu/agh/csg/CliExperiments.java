package pl.edu.agh.csg;

import org.cloudbus.cloudsim.cloudlets.Cloudlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Code used to generate different data points from static tests on the KTH-SP2 dataset
 */
class CliExperiments {
    public static void main(String[] args) {
        try {
            SimulationEnvironment simulationEnvironment = new SimulationEnvironment("KTH-SP2-1996-2.1-cln_50.swf");

            List<Double> results = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                results.add(simulateComplete(simulationEnvironment));
            }

            System.out.println(Arrays.toString(results.toArray()));

            simulationEnvironment.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static double simulateComplete(SimulationEnvironment simulationEnvironment) throws IOException, InterruptedException {
        simulationEnvironment.reset();
        double totalReward = 0.0;
        double totalWaitTime = 0.0;
        while (true) {
            SimulationStepResult stepResult = simulationEnvironment.step(0);
            totalReward += stepResult.getReward();
            totalWaitTime += stepResult.getObs()[5];

            final long start = System.nanoTime();
            String env = simulationEnvironment.render();
            final long stop = System.nanoTime();
            System.out.println("Render time: " + ((stop - start) / 1000000000.0) + "s");
            System.out.println("Render size: " + env.length());

            System.out.println("Observations: " + Arrays.toString(stepResult.getObs()) + " reward: " + stepResult.getReward());
            if (stepResult.isDone()) {
                System.out.println(">>> SIMULATION FINISHED <<<");
                break;
            }
        }

        for (Cloudlet job : simulationEnvironment.getJobs()) {
            if (job.getStatus() != Cloudlet.Status.SUCCESS) {
                System.out.println("ERROR: Cloudlet: " + job.getId() + " status: " + job.getStatus() + " required PEs: " + job.getNumberOfPes());
            }
        }
        System.out.println("Total reward: " + totalReward + " total wait time: " + totalWaitTime);
        return totalReward;
    }
}
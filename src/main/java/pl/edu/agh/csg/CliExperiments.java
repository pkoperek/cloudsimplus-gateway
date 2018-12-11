package pl.edu.agh.csg;

import org.cloudbus.cloudsim.cloudlets.Cloudlet;

import java.io.IOException;
import java.util.*;

/**
 * Code used to generate different data points from static tests on the KTH-SP2 dataset
 */
class CliExperiments {
    public static void main(String[] args) {
        try {
            SimulationEnvironment simulationEnvironment = new SimulationEnvironment();

            List<Double> results = new ArrayList<>();
            for (int i = 0; i < 1; i++) {
                results.add(simulateCompleteEpisode(simulationEnvironment));
            }

            System.out.println(Arrays.toString(results.toArray()));

            simulationEnvironment.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static double simulateCompleteEpisode(SimulationEnvironment simulationEnvironment) throws IOException, InterruptedException {
        Map<String, String> parameters = new HashMap<>();

        // 1 minute time window
//        parameters.put("START_TIME", "1544026148002");
//        parameters.put("END_TIME", "1544026148062");
        simulationEnvironment.reset(parameters);
        double totalReward = 0.0;
        double totalWaitTime = 0.0;

        boolean added = false;
        int stepsCnt = 0;
        while (true) {
            int action = 0;
            if(!added) action = 1;
            added = true;

            SimulationStepResult stepResult = simulationEnvironment.step(action);
            stepsCnt++;
            totalReward += stepResult.getReward();
            totalWaitTime += stepResult.getObs()[5];

            final long start = System.nanoTime();
            String env = simulationEnvironment.render();
            final long stop = System.nanoTime();
            System.out.println("Render time: " + ((stop - start) / 1000000000.0) + "s");
            System.out.println("Render size: " + env.length());
            System.out.println("Total reward: " + totalReward + " total wait: " + totalWaitTime);
            System.out.println("Observations: " + Arrays.toString(stepResult.getObs()) + " reward: " + stepResult.getReward());
            if (stepResult.isDone()) {
                System.out.println(">>> SIMULATION FINISHED <<< >>> Steps " + stepsCnt + " <<<");
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
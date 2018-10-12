package pl.edu.agh.csg;

import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import pl.edu.agh.csg.SimulationEnvironment;
import pl.edu.agh.csg.SimulationStepResult;

import java.io.IOException;
import java.util.Arrays;

/**
 * Code used to generate different data points from static tests on the KTH-SP2 dataset
 */
class CliExperiments {
    public static void main(String[] args) {
        try {
            SimulationEnvironment simulationEnvironment = new SimulationEnvironment("KTH-SP2-1996-2.1-cln_250.swf");
            simulationEnvironment.reset();
            double totalReward = 0.0;
            double totalWaitTime = 0.0;
            int iteration = 0;
            int action = 0;
            while (true) {

                if(iteration % 10 == 3) {
                    action = 2;
                } else {
                    action = 0;
                }

                SimulationStepResult stepResult = simulationEnvironment.step(action);
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
                iteration++;
            }

            for(Cloudlet job : simulationEnvironment.getJobs()) {
                System.out.println("Cloudlet: " + job.getId() + " status: " + job.getStatus() + " required PEs: " + job.getNumberOfPes());
            }

            simulationEnvironment.close();
            System.out.println("Total reward: " + totalReward + " total wait time: " + totalWaitTime);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
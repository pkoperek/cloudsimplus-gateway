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
            SimulationEnvironment simulationEnvironment = new SimulationEnvironment("KTH-SP2-1996-2.1-cln.swf");
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

            for(Cloudlet job : simulationEnvironment.getJobs()) {
                if(job.getStatus() != Cloudlet.Status.SUCCESS) {
                    System.out.println("ERROR: Cloudlet: " + job.getId() + " status: " + job.getStatus() + " required PEs: " + job.getNumberOfPes());
                }
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
package pl.edu.agh.csg;

import java.util.Arrays;

public class SimulationStepResult {

    private final boolean done;
    private final double[] obs;
    private final double reward;

    public SimulationStepResult(boolean done, double[] obs, double reward) {
        this.done = done;
        this.obs = obs;
        this.reward = reward;
    }

    public boolean isDone() {
        return done;
    }

    public double[] getObs() {
        return obs;
    }

    public double getReward() {
        return reward;
    }

    @Override
    public String toString() {
        return "SimulationStepResult{" +
                "done=" + done +
                ", obs=" + Arrays.toString(obs) +
                ", reward=" + reward +
                '}';
    }
}

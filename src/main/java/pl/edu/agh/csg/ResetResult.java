package pl.edu.agh.csg;

public class ResetResult {
    private final double[] obs;

    public ResetResult(double[] obs) {
        this.obs = obs;
    }

    public double[] getObs() {
        return this.obs;
    }
}
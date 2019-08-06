package pl.edu.agh.csg;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.NotImplementedException;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimulationFactory {

    private static final Logger logger = LoggerFactory.getLogger(SimulationFactory.class.getName());
    private static final Type cloudletDescriptors = new TypeToken<List<CloudletDescriptor>>() {}.getType();

    public static final String SIMULATION_SPEEDUP = "SIMULATION_SPEEDUP";
    public static final String SIMULATION_SPEEDUP_DEFAULT = "1.0";

    public static final String INITIAL_VM_COUNT = "INITIAL_VM_COUNT";
    public static final String INITIAL_VM_COUNT_DEFAULT = "10";

    public static final String SOURCE_OF_JOBS_PARAMS = "PARAMS";
    public static final String SOURCE_OF_JOBS_PARAMS_JOBS = "JOBS";
    public static final String SOURCE_OF_JOBS_FILE = "FILE";
    public static final String SOURCE_OF_JOBS_DATABASE = "DB";
    public static final String SOURCE_OF_JOBS = "SOURCE_OF_JOBS";
    public static final String SOURCE_OF_JOBS_DEFAULT = SOURCE_OF_JOBS_PARAMS;

    private static final Gson gson = new Gson();

    private int created = 0;

    public synchronized WrappedSimulation create(Map<String, String> maybeParameters) {
        String identifier = "Sim" + created;
        this.created++;

        // get number of initial vms in
        final String initialVmCountStr = maybeParameters.getOrDefault(INITIAL_VM_COUNT, INITIAL_VM_COUNT_DEFAULT);
        final int initialVmCount = Integer.parseInt(initialVmCountStr);

        final String simulationSpeedUpStr = maybeParameters.getOrDefault(SIMULATION_SPEEDUP, SIMULATION_SPEEDUP_DEFAULT);
        final double simulationSpeedUp = Double.valueOf(simulationSpeedUpStr);

        final String sourceOfJobs = maybeParameters.getOrDefault(SOURCE_OF_JOBS, SOURCE_OF_JOBS_DEFAULT);

        final List<Cloudlet> jobs;

        switch (sourceOfJobs) {
            case SOURCE_OF_JOBS_DATABASE:
                jobs = loadJobsFromDatabase(maybeParameters);
                break;
            case SOURCE_OF_JOBS_FILE:
                jobs = loadJobsFromFile(maybeParameters);
                break;
            case SOURCE_OF_JOBS_PARAMS:
                // fall-through
            default:
                jobs = loadJobsFromParams(maybeParameters, simulationSpeedUp);
        }

        return new WrappedSimulation(identifier, initialVmCount, simulationSpeedUp, jobs);
    }

    private List<Cloudlet> loadJobsFromParams(Map<String, String> maybeParameters, double simulationSpeedUp) {
        List<Cloudlet> retVal = new ArrayList<>();
        final String jobsAsJson = maybeParameters.get(SOURCE_OF_JOBS_PARAMS_JOBS);

        final List<CloudletDescriptor> deserialized = gson.fromJson(jobsAsJson, cloudletDescriptors);

        for (CloudletDescriptor cloudletDescriptor : deserialized) {
            retVal.add(speedUp(cloudletDescriptor, simulationSpeedUp).toCloudlet());
        }

        logger.info("Deserialized " + retVal.size() + " jobs");

        return retVal;
    }

    private CloudletDescriptor speedUp(CloudletDescriptor cloudletDescriptor, double simulationSpeedUp) {
        final long newMi = (long) (cloudletDescriptor.getMi() / simulationSpeedUp);
        final long mi = newMi == 0 ? 1 : newMi;
        final long submissionDelay = (long) (cloudletDescriptor.getSubmissionDelay() / simulationSpeedUp);
        return new CloudletDescriptor(
                cloudletDescriptor.getJobId(),
                submissionDelay,
                mi,
                cloudletDescriptor.getNumberOfCores()
        );
    }

    private List<Cloudlet> loadJobsFromDatabase(Map<String, String> maybeParameters) {
        throw new NotImplementedException("Feature not implemented yet!");
    }

    private List<Cloudlet> loadJobsFromFile(Map<String, String> maybeParameters) {
        throw new NotImplementedException("Feature not implemented yet!");
    }

}

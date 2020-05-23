package pl.edu.agh.csg;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimulationFactory {

    private static final Logger logger = LoggerFactory.getLogger(SimulationFactory.class.getName());
    private static final Type cloudletDescriptors = new TypeToken<List<CloudletDescriptor>>() {}.getType();

    public static final String SPLIT_LARGE_JOBS = "SPLIT_LARGE_JOBS";
    public static final String SPLIT_LARGE_JOBS_DEFAULT = "true";

    public static final String QUEUE_WAIT_PENALTY = "QUEUE_WAIT_PENALTY";
    public static final String QUEUE_WAIT_PENALTY_DEFAULT = "0.00001";

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

        final String queueWaitPenaltyStr = maybeParameters.getOrDefault(QUEUE_WAIT_PENALTY, QUEUE_WAIT_PENALTY_DEFAULT);
        final double queueWaitPenalty = Double.valueOf(queueWaitPenaltyStr);

        final String splitLargeJobsStr = maybeParameters.getOrDefault(SPLIT_LARGE_JOBS, SPLIT_LARGE_JOBS_DEFAULT);
        final boolean splitLargeJobs = Boolean.valueOf(splitLargeJobsStr.toLowerCase());

        final List<CloudletDescriptor> jobs;

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

        final SimulationSettings settings = new SimulationSettings();
        final List<CloudletDescriptor> splitted;
        if(splitLargeJobs) {
            logger.info("Splitting large jobs");
            splitted = splitLargeJobs(jobs, settings);
        } else {
            logger.info("Not applying the splitting algorithm - using raw jobs");
            splitted = jobs;
        }

        return new WrappedSimulation(settings, identifier, initialVmCount, simulationSpeedUp, queueWaitPenalty, splitted);
    }

    private List<CloudletDescriptor> splitLargeJobs(List<CloudletDescriptor> jobs, SimulationSettings settings) {
        final int hostPeCnt = (int) settings.getBasicVmPeCnt();

        int splittedId = jobs.size() * 10;

        List<CloudletDescriptor> splitted = new ArrayList<>();
        for(CloudletDescriptor cloudletDescriptor : jobs) {
            int numberOfCores = cloudletDescriptor.getNumberOfCores();
            if(numberOfCores <= hostPeCnt) {
                splitted.add(cloudletDescriptor);
            } else {
                final long miPerCore = (cloudletDescriptor.getMi() / cloudletDescriptor.getNumberOfCores());
                while(numberOfCores > 0) {
                    final int fractionOfCores = hostPeCnt < numberOfCores ? hostPeCnt : numberOfCores;
                    final long totalMips = miPerCore * fractionOfCores;
                    final long totalMipsNotZero = totalMips == 0 ? 1 : totalMips;
                    CloudletDescriptor splittedDescriptor = new CloudletDescriptor(
                            splittedId,
                            cloudletDescriptor.getSubmissionDelay(),
                            totalMipsNotZero,
                            fractionOfCores);
                    splitted.add(splittedDescriptor);
                    splittedId++;
                    numberOfCores -= hostPeCnt;
                }
            }
        }

        logger.info("Splitted: " + jobs.size() + " into " + splitted.size());

        return splitted;
    }

    private List<CloudletDescriptor> loadJobsFromParams(Map<String, String> maybeParameters, double simulationSpeedUp) {
        List<CloudletDescriptor> retVal = new ArrayList<>();
        final String jobsAsJson = maybeParameters.get(SOURCE_OF_JOBS_PARAMS_JOBS);

        final List<CloudletDescriptor> deserialized = gson.fromJson(jobsAsJson, cloudletDescriptors);

        for (CloudletDescriptor cloudletDescriptor : deserialized) {
            retVal.add(speedUp(cloudletDescriptor, simulationSpeedUp));
        }

        logger.info("Deserialized " + retVal.size() + " jobs");

        return retVal;
    }

    private CloudletDescriptor speedUp(CloudletDescriptor cloudletDescriptor, double simulationSpeedUp) {
        final long newMi = (long) (cloudletDescriptor.getMi() / simulationSpeedUp);
        final long mi = newMi == 0 ? 1 : newMi;
        final long submissionDelayReal = cloudletDescriptor.getSubmissionDelay() < 0 ? 0L : cloudletDescriptor.getSubmissionDelay();
        final long submissionDelay = (long) (submissionDelayReal / simulationSpeedUp);
        final int numberOfCores = cloudletDescriptor.getNumberOfCores() < 0 ? 1 : cloudletDescriptor.getNumberOfCores();
        return new CloudletDescriptor(
                cloudletDescriptor.getJobId(),
                submissionDelay,
                mi,
                numberOfCores
        );
    }

    private List<CloudletDescriptor> loadJobsFromDatabase(Map<String, String> maybeParameters) {
        throw new NotImplementedException("Feature not implemented yet!");
    }

    private List<CloudletDescriptor> loadJobsFromFile(Map<String, String> maybeParameters) {
        throw new NotImplementedException("Feature not implemented yet!");
    }

}

package pl.edu.agh.csg;

import org.apache.commons.lang3.ArrayUtils;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletExecution;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.events.SimEvent;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class CloudSimProxy {

    public static final String SMALL = "S";
    public static final String MEDIUM = "M";
    public static final String LARGE = "L";

    private static final Logger logger = LoggerFactory.getLogger(CloudSimProxy.class.getName());
    private static final Double[] double_arr = new Double[0];

    private final DatacenterBroker broker;
    private final CloudSim cloudSim;
    private final SimulationSettings settings;
    private final VmCost vmCost;
    private final Datacenter datacenter;
    private final double simulationSpeedUp;
    private final Map<Long, Double> originalSubmissionDelay = new HashMap<>();
    private final Random random = new Random(System.currentTimeMillis());
    private final List<Cloudlet> jobs = new ArrayList<>();
    private final List<Cloudlet> potentiallyWaitingJobs = new ArrayList<>(1024);
    private final List<Cloudlet> alreadyStarted = new ArrayList<>(128);
    private int toAddJobId = 0;
    private int previousIntervalJobId = 0;
    private int nextVmId;

    public CloudSimProxy(SimulationSettings settings, int initialVmCount, List<Cloudlet> inputJobs, double simulationSpeedUp) {
        this.settings = settings;
        this.cloudSim = new CloudSim(0.1);
        this.broker = createDatacenterBroker();
        this.datacenter = createDatacenter();
        this.vmCost = new VmCost(settings.getVmRunningHourlyCost(), simulationSpeedUp);
        this.simulationSpeedUp = simulationSpeedUp;

        this.nextVmId = 0;

        final List<? extends Vm> smallVmList = createVmList(initialVmCount, SMALL);
        final List<? extends Vm> mediumVmList = createVmList(initialVmCount, MEDIUM);
        final List<? extends Vm> largeVmList = createVmList(initialVmCount, LARGE);
        broker.submitVmList(smallVmList);
        broker.submitVmList(mediumVmList);
        broker.submitVmList(largeVmList);

        this.jobs.addAll(inputJobs);
        Collections.sort(this.jobs, new DelayCloudletComparator());
        this.jobs.forEach(c -> originalSubmissionDelay.put(c.getId(), c.getSubmissionDelay()));

        scheduleAdditionalCloudletProcessingEvent(this.jobs);

        this.cloudSim.startSync();
        this.runFor(0.1);
    }

    private void scheduleAdditionalCloudletProcessingEvent(final List<Cloudlet> jobs) {
        // a second after every cloudlet will be submitted we add an event - this should prevent
        // the simulation from ending while we have some jobs to schedule
        jobs.forEach(c ->
                this.cloudSim.send(
                        datacenter,
                        datacenter,
                        c.getSubmissionDelay() + 1.0,
                        CloudSimTags.VM_UPDATE_CLOUDLET_PROCESSING,
                        ResubmitAnchor.THE_VALUE
                )
        );
    }

    private Datacenter createDatacenter() {
        List<Host> hostList = new ArrayList<>();

        for (int i = 0; i < settings.getDatacenterHostsCnt(); i++) {
            List<Pe> peList = createPeList();

            final long hostRam = settings.getHostRam();
            final long hostBw = settings.getHostBw();
            final long hostSize = settings.getHostSize();
            Host host =
                    new HostSimple(hostRam, hostBw, hostSize, peList)
                            .setRamProvisioner(new ResourceProvisionerSimple())
                            .setBwProvisioner(new ResourceProvisionerSimple())
                            .setVmScheduler(new VmSchedulerTimeShared());

            hostList.add(host);
        }

        return new DatacenterSimple(cloudSim, hostList, new VmAllocationPolicySimple());
    }

    private List<? extends Vm> createVmList(int vmCount, String type) {
        List<Vm> vmList = new ArrayList<>(vmCount);

        for (int i = 0; i < vmCount; i++) {
            // 1 VM == 1 HOST for simplicity
            vmList.add(createVmWithId(type));
        }

        return vmList;
    }

    private Vm createVmWithId(String type) {
        int sizeMultiplier = getSizeMultiplier(type);

        Vm vm = new VmSimple(
                this.nextVmId,
                settings.getHostPeMips(),
                settings.getBasicVmPeCnt() * sizeMultiplier);
        this.nextVmId++;
        vm
                .setRam(settings.getBasicVmRam() * sizeMultiplier)
                .setBw(settings.getBasicVmBw())
                .setSize(settings.getBasicVmSize())
                .setCloudletScheduler(new CloudletScheduler())
                .setDescription(type);
        vmCost.notifyCreateVM(vm);
        return vm;
    }

    private int getSizeMultiplier(String type) {
        int sizeMultiplier;

        switch (type) {
            case MEDIUM:
                sizeMultiplier = 2; // m5a.xlarge
                break;
            case LARGE:
                sizeMultiplier = 4; // m5a.2xlarge
                break;
            case SMALL:
            default:
                sizeMultiplier = 1; // m5a.large
        }
        return sizeMultiplier;
    }

    private List<Pe> createPeList() {
        List<Pe> peList = new ArrayList<>();
        for (int i = 0; i < settings.getHostPeCnt(); i++) {
            peList.add(new PeSimple(settings.getHostPeMips(), new PeProvisionerSimple()));
        }

        return peList;
    }

    private DatacenterBrokerSimple createDatacenterBroker() {
        return new DatacenterBrokerSimple(cloudSim);
    }

    public void runFor(final double interval) {
        final double target = this.cloudSim.clock() + interval;

        scheduleJobsUntil(target);

        int i = 0;
        double adjustedInterval = interval;
        while (this.cloudSim.runFor(adjustedInterval) < target) {
            adjustedInterval = target - this.cloudSim.clock();
            adjustedInterval = adjustedInterval <= 0 ? cloudSim.getMinTimeBetweenEvents() : adjustedInterval;

            // Force stop if something runs out of control
            if (i >= 10000) {
                throw new RuntimeException("Breaking a really long loop in runFor!");
            }
            i++;
        }

        alreadyStarted.clear();

        final Iterator<Cloudlet> iterator = potentiallyWaitingJobs.iterator();
        while (iterator.hasNext()) {
            Cloudlet job = iterator.next();
            if (job.getStatus() == Cloudlet.Status.INEXEC || job.getStatus() == Cloudlet.Status.SUCCESS || job.getStatus() == Cloudlet.Status.CANCELED) {
                alreadyStarted.add(job);
                iterator.remove();
            }
        }

        cancelInvalidEvents();
        printJobStatsAfterEndOfSimulation();
    }

    private void printJobStatsAfterEndOfSimulation() {
        if (!cloudSim.isRunning()) {
            logger.info("End of simulation, some reality check stats:");
            logger.info("all jobs: " + this.jobs.size());

            Map<Cloudlet.Status, Integer> countByStatus = new HashMap<>();
            for (Cloudlet c : this.jobs) {
                final Cloudlet.Status status = c.getStatus();
                int count = countByStatus.getOrDefault(status, 0);
                countByStatus.put(status, count + 1);
            }

            for(Map.Entry<Cloudlet.Status, Integer> e : countByStatus.entrySet()) {
                logger.info(e.getKey().toString() + ": " + e.getValue());
            }

            logger.info("Jobs which are still queued");
            for(Cloudlet c : this.jobs) {
                if(Cloudlet.Status.QUEUED.equals(c.getStatus())) {
                    logger.info("Cloudlet: " + c.getId());
                    logger.info("Number of PEs: " + c.getNumberOfPes());
                    logger.info("Number of MIPS: " + c.getLength());
                    logger.info("Submission delay: " + c.getSubmissionDelay());
                    final Vm vm = c.getVm();
                    logger.info("VM: " + vm.getId() + "(" + vm.getDescription() + ")"
                            + " CPU: " + vm.getNumberOfPes() +"/" + vm.getMips() +  " @ " + vm.getCpuPercentUtilization()
                            + " RAM: " + vm.getRam().getAllocatedResource());
                }
            }

        }
    }

    private void cancelInvalidEvents() {
        final long clock = (long) cloudSim.clock();

        if (clock % 100 == 0) {
            logger.warn("Cleaning up events (before): " + getNumberOfFutureEvents());
            cloudSim.cancelAll(datacenter, new Predicate<SimEvent>() {

                private SimEvent previous;

                @Override
                public boolean test(SimEvent current) {
                    // remove dupes
                    if (previous != null &&
                            current.getTag() == CloudSimTags.VM_UPDATE_CLOUDLET_PROCESSING &&
                            current.getSource() == datacenter &&
                            current.getDestination() == datacenter &&
                            previous.getTime() == current.getTime() &&
                            current.getData() == ResubmitAnchor.THE_VALUE
                    ) {
                        return true;
                    }

                    previous = current;
                    return false;
                }
            });
            logger.warn("Cleaning up events (after): " + getNumberOfFutureEvents());
        }
    }

    private void scheduleJobsUntil(double target) {
        previousIntervalJobId = nextVmId;
        List<Cloudlet> jobsToSubmit = new ArrayList<>();

        long addedMips = 0;
        while (toAddJobId < this.jobs.size() && this.jobs.get(toAddJobId).getSubmissionDelay() <= target) {
            // we process every cloudlet only once here...
            final Cloudlet cloudlet = this.jobs.get(toAddJobId);

            // the job shold enter the cluster once target is crossed
            cloudlet.setSubmissionDelay(1.0);
            jobsToSubmit.add(cloudlet);
            addedMips += cloudlet.getTotalLength();
            toAddJobId++;
        }

        final double step = this.clock();
        logger.debug("Job submission:  " + step + ", " + jobsToSubmit.size() + ", " + addedMips);

        if (jobsToSubmit.size() > 0) {
            broker.submitCloudletList(jobsToSubmit);
            potentiallyWaitingJobs.addAll(jobsToSubmit);
        }
    }

    public boolean isRunning() {
        return cloudSim.isRunning();
    }

    public double getNumberOfActiveVMs() {
        return (double) broker.getVmExecList().size();
    }

    public long getNumberOfActiveCores() {
        final Optional<Long> reduce = this.broker
                .getVmExecList()
                .parallelStream()
                .map(Vm::getNumberOfPes)
                .reduce(Long::sum);
        return reduce.orElse(0L);
    }

    public double[] getVmCpuUsage() {
        List<Vm> input = broker.getVmExecList();
        double[] cpuPercentUsage = new double[input.size()];
        int i = 0;
        for (Vm vm : input) {
            cpuPercentUsage[i] = vm.getCpuPercentUtilization();
            i++;
        }

        return cpuPercentUsage;
    }

    public int getSubmittedJobsCountLastInterval() {
        return toAddJobId - previousIntervalJobId;
    }

    public int getWaitingJobsCountInterval(double interval) {
        double start = clock() - interval;

        int jobsWaitingSubmittedInTheInterval = 0;
        for (Cloudlet cloudlet : potentiallyWaitingJobs) {
            if (!cloudlet.getStatus().equals(Cloudlet.Status.INEXEC)) {
                double systemEntryTime = this.originalSubmissionDelay.get(cloudlet.getId());
                if (systemEntryTime >= start) {
                    jobsWaitingSubmittedInTheInterval++;
                }
            }
        }
        return jobsWaitingSubmittedInTheInterval;
    }

    public int getSubmittedJobsCount() {
        // this is incremented every time job is submitted
        return this.toAddJobId;
    }

    public double[] getVmMemoryUsage() {
        List<Vm> input = broker.getVmExecList();
        double[] memPercentUsage = new double[input.size()];
        int i = 0;
        for (Vm vm : input) {
            memPercentUsage[i] = vm.getRam().getPercentUtilization();
        }
        return memPercentUsage;
    }

    public double[] getWaitTimesFromLastInterval() {
        List<Double> waitingTimes = new ArrayList<>();
        for (Cloudlet cloudlet : this.potentiallyWaitingJobs) {
            double systemEntryTime = this.originalSubmissionDelay.get(cloudlet.getId());
            double realWaitingTime = cloudSim.clock() - systemEntryTime;
            waitingTimes.add(realWaitingTime);
        }

        return ArrayUtils.toPrimitive(waitingTimes.toArray(double_arr));
    }

    public void addNewVM(String type) {
        // assuming average delay up to 97s as in 10.1109/CLOUD.2012.103
        // from anecdotal exp the startup time can be as fast as 45s
        Vm newVm = createVmWithId(type);
        double delay = (45 + Math.random() * 52) / this.simulationSpeedUp;
        newVm.setSubmissionDelay(delay);

        broker.submitVm(newVm);
        logger.debug("VM creating requested, delay: " + delay + " type: " + type);
    }

    public boolean removeRandomlyVM(String type) {
        List<Vm> vmExecList = broker.getVmExecList();

        List<Vm> vmsOfType = vmExecList
                .parallelStream()
                .filter(vm -> type.equals((vm.getDescription())))
                .collect(Collectors.toList());

        if (canKillVm(type, vmsOfType.size())) {
            int vmToKillIdx = random.nextInt(vmsOfType.size());
            destroyVm(vmsOfType.get(vmToKillIdx));
            return true;
        } else {
            logger.warn("Can't kill a VM - only one running");
            return false;
        }
    }

    private boolean canKillVm(String type, int size) {
        if (SMALL.equals(type)) {
            return size > 1;
        }

        return size > 0;
    }

    private void destroyVm(Vm vm) {
        final String vmSize = vm.getDescription();
        final List<Cloudlet> affectedCloudlets = this.broker.destroyVm(vm);
        logger.debug("Killing VM: "
                + vm.getId()
                + " to reschedule cloudlets: "
                + affectedCloudlets.size()
                + " type: "
                + vmSize);
        rescheduleCloudlets(affectedCloudlets);
    }

    private void rescheduleCloudlets(List<Cloudlet> affectedCloudlets) {
        final double currentClock = cloudSim.clock();

        affectedCloudlets.forEach(cloudlet -> {
            Double submissionDelay = originalSubmissionDelay.get(cloudlet.getId());

            if (submissionDelay == null) {
                throw new RuntimeException("Cloudlet with ID: " + cloudlet.getId() + " not seen previously! Original submission time unknown!");
            }

            if (submissionDelay < currentClock) {
                submissionDelay = 1.0;
            } else {
                // if we the Cloudlet still hasn't been started, let it start at the scheduled time.
                submissionDelay -= currentClock;
            }

            cloudlet.setSubmissionDelay(submissionDelay);
        });

        broker.submitCloudletList(affectedCloudlets);

        scheduleAdditionalCloudletProcessingEvent(affectedCloudlets);
    }

    public double clock() {
        return this.cloudSim.clock();
    }

    public long getNumberOfFutureEvents() {
        return this.cloudSim.getNumberOfFutureEvents(simEvent -> true);
    }

    public int getWaitingJobsCount() {
        return this.potentiallyWaitingJobs.size();
    }

    public double getRunningCost() {
        return vmCost.getVMCostPerSecond(this.clock());
    }

    class CloudletScheduler extends CloudletSchedulerSpaceShared {
        @Override
        public double updateProcessing(double currentTime, List<Double> mipsShare) {
            final int sizeBefore = this.getCloudletWaitingList().size();
            final double nextSimulationTime = super.updateProcessing(currentTime, mipsShare);
            final int sizeAfter = this.getCloudletWaitingList().size();

            // if we have a new cloudlet being processed, schedule another recalculation, which should trigger a proper
            // estimation of end time
            if (sizeAfter != sizeBefore && Double.MAX_VALUE == nextSimulationTime) {
                return this.getVm().getSimulation().getMinTimeBetweenEvents();
            }

            return nextSimulationTime;
        }

        @Override
        protected Optional<CloudletExecution> findSuitableWaitingCloudlet() {
            if (getVm().getProcessor().getAvailableResource() > 0) {
                final List<CloudletExecution> cloudletWaitingList = getCloudletWaitingList();
                for (CloudletExecution cle : cloudletWaitingList) {
                    if (this.isThereEnoughFreePesForCloudlet(cle)) {
                        return Optional.of(cle);
                    }
                }
            }

            return Optional.empty();
        }
    }

    class DelayCloudletComparator implements Comparator<Cloudlet> {

        @Override
        public int compare(Cloudlet left, Cloudlet right) {
            final double diff = left.getSubmissionDelay() - right.getSubmissionDelay();
            if (diff < 0) {
                return -1;
            }

            if (diff > 0) {
                return 1;
            }
            return 0;
        }
    }
}

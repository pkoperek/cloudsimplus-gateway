package pl.edu.agh.csg;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletExecution;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.events.SimEvent;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.listeners.CloudletVmEventInfo;
import org.cloudsimplus.listeners.EventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CloudSimProxy {

    public static final String SMALL = "S";
    public static final String MEDIUM = "M";
    public static final String LARGE = "L";

    private static final Logger logger = LoggerFactory.getLogger(CloudSimProxy.class.getName());

    private final DatacenterBrokerFirstFitFixed broker;
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
    private final Set<Long> finishedIds = new HashSet<>();
    private int toAddJobId = 0;
    private int previousIntervalJobId = 0;
    private int nextVmId;

    public CloudSimProxy(SimulationSettings settings,
                         Map<String, Integer> initialVmsCount,
                         List<Cloudlet> inputJobs,
                         double simulationSpeedUp) {
        this.settings = settings;
        this.cloudSim = new CloudSim(0.01);
        this.broker = createDatacenterBroker();
        this.datacenter = createDatacenter();
        this.vmCost = new VmCost(settings.getVmRunningHourlyCost(),
                simulationSpeedUp,
                settings.isPayingForTheFullHour());
        this.simulationSpeedUp = simulationSpeedUp;

        this.nextVmId = 0;

        final List<? extends Vm> smallVmList = createVmList(initialVmsCount.get(SMALL), SMALL);
        final List<? extends Vm> mediumVmList = createVmList(initialVmsCount.get(MEDIUM), MEDIUM);
        final List<? extends Vm> largeVmList = createVmList(initialVmsCount.get(LARGE), LARGE);
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

    public boolean allJobsFinished() {
        return this.finishedIds.size() == this.jobs.size();
    }

    public int getFinishedCount() {
        return finishedIds.size();
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
                    new HostWithoutCreatedList(hostRam, hostBw, hostSize, peList)
                            .setRamProvisioner(new ResourceProvisionerSimple())
                            .setBwProvisioner(new ResourceProvisionerSimple())
                            .setVmScheduler(new VmSchedulerTimeShared());

            hostList.add(host);
        }

        return new LoggingDatacenter(cloudSim, hostList, new VmAllocationPolicySimple());
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
                .setCloudletScheduler(new OptimizedCloudletScheduler())
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

    private DatacenterBrokerFirstFitFixed createDatacenterBroker() {
        // this should be first fit
        return new DatacenterBrokerFirstFitFixed(cloudSim);
    }

    public void runFor(final double interval) {
        if(!this.isRunning()) {
            throw new RuntimeException("The simulation is not running - please reset or create a new one!");
        }

        long start = System.nanoTime();
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

        if(shouldPrintJobStats()) {
            printJobStats();
        }

        // the size of cloudletsCreatedList grows to huge numbers
        // as we re-schedule cloudlets when VMs get killed
        // to avoid OOMing we need to clear that list
        // it is a safe operation in our environment, because that list is only used in
        // CloudSim+ when a VM is being upscaled (we don't do that)
        if(!settings.isStoreCreatedCloudletsDatacenterBroker()) {
            this.broker.getCloudletCreatedList().clear();
        }

        long end = System.nanoTime();
        long diff = end - start;
        double diffInSec = ((double)diff) / 1_000_000_000L;

        // TODO: can be removed after validating the fix of OOM
        // should always be zero
        final int debugBrokerCreatedListSize = this.broker.getCloudletCreatedList().size();
        logger.debug("runFor (" + this.clock() + ") took " + diff + "ns / " + diffInSec + "s (DEBUG: " + debugBrokerCreatedListSize + ")");
    }

    private boolean shouldPrintJobStats() {
        return this.settings.getPrintJobsPeriodically() && Double.valueOf(this.clock()).longValue() % 20000 == 0;
    }

    private void printJobStatsAfterEndOfSimulation() {
        if (!this.isRunning()) {
            logger.info("End of simulation, some reality check stats:");

            printJobStats();
        }
    }

    public void printJobStats() {
        logger.info("All jobs: " + this.jobs.size());
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
        for(Cloudlet cloudlet : this.jobs) {
            if(Cloudlet.Status.QUEUED.equals(cloudlet.getStatus())) {
                printCloudlet(cloudlet);
            }
        }
        logger.info("Jobs which are still executed");
        for(Cloudlet cloudlet : this.jobs) {
            if(Cloudlet.Status.INEXEC.equals(cloudlet.getStatus())) {
                printCloudlet(cloudlet);
            }
        }
    }

    private void printCloudlet(Cloudlet cloudlet) {
        logger.info("Cloudlet: " + cloudlet.getId());
        logger.info("Number of PEs: " + cloudlet.getNumberOfPes());
        logger.info("Number of MIPS: " + cloudlet.getLength());
        logger.info("Submission delay: " + cloudlet.getSubmissionDelay());
        logger.info("Started: " + cloudlet.getExecStartTime());
        final Vm vm = cloudlet.getVm();
        logger.info("VM: " + vm.getId() + "(" + vm.getDescription() + ")"
                + " CPU: " + vm.getNumberOfPes() + "/" + vm.getMips() + " @ " + vm.getCpuPercentUtilization()
                + " RAM: " + vm.getRam().getAllocatedResource()
                + " Start time: " + vm.getStartTime()
                + " Stop time: " + vm.getStopTime());
    }

    private void cancelInvalidEvents() {
        final long clock = (long) cloudSim.clock();

        if (clock % 100 == 0) {
            logger.debug("Cleaning up events (before): " + getNumberOfFutureEvents());
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
            logger.debug("Cleaning up events (after): " + getNumberOfFutureEvents());
        }
    }

    private void scheduleJobsUntil(double target) {
        previousIntervalJobId = nextVmId;
        List<Cloudlet> jobsToSubmit = new ArrayList<>();

        while (toAddJobId < this.jobs.size() && this.jobs.get(toAddJobId).getSubmissionDelay() <= target) {
            // we process every cloudlet only once here...
            final Cloudlet cloudlet = this.jobs.get(toAddJobId);

            // the job shold enter the cluster once target is crossed
            cloudlet.setSubmissionDelay(1.0);
            cloudlet.addOnFinishListener(new EventListener<CloudletVmEventInfo>() {
                @Override
                public void update(CloudletVmEventInfo info) {
                    logger.debug("Cloudlet: " + cloudlet.getId() + "/" + cloudlet.getVm().getId()
                            + " Finished.");
                    finishedIds.add(info.getCloudlet().getId());
                }
            });
            jobsToSubmit.add(cloudlet);
            toAddJobId++;
        }

        if (jobsToSubmit.size() > 0) {
            submitCloudletsList(jobsToSubmit);
            potentiallyWaitingJobs.addAll(jobsToSubmit);
        }

        int pctSubmitted = (int) ((toAddJobId / (double) this.jobs.size()) * 10000d);
        int upper = pctSubmitted / 100;
        int lower = pctSubmitted % 100;
        logger.info(
                "Simulation progress: submitted: " + upper + "." + lower + "% "
                        + "Waiting list size: " + this.broker.getCloudletWaitingList().size());
    }

    private void submitCloudletsList(List<Cloudlet> jobsToSubmit) {
        logger.debug("Submitting: " + jobsToSubmit.size() + " jobs");
        broker.submitCloudletList(jobsToSubmit);

        // we immediately clear up that list because it is not really
        // used anywhere but traversing it takes a lot of time
        broker.getCloudletSubmittedList().clear();
    }

    public boolean isRunning() {
        // if we don't have unfinished jobs, it doesn't make sense to execute
        // any actions
        return cloudSim.isRunning() && hasUnfinishedJobs();
    }

    private boolean hasUnfinishedJobs() {
        return this.finishedIds.size() < this.jobs.size();
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

    private Cloudlet resetCloudlet(Cloudlet cloudlet) {
        cloudlet.setVm(Vm.NULL);
        return cloudlet.reset();
    }

    private List<Cloudlet> resetCloudlets(List<CloudletExecution> cloudlets) {
        return cloudlets
                .stream()
                .map(CloudletExecution::getCloudlet)
                .map(this::resetCloudlet)
                .collect(Collectors.toList());
    }

    private void destroyVm(Vm vm) {
        final String vmSize = vm.getDescription();

        // TODO: okazało się, że jak czyścimy submitted list to chyba "zapominamy" jakieś cloudlety wykonać
        // wydaje mi się, że jest po prostu jakaś lista w schedulerze o jakiej zapominamy i trzeba

        // replaces broker.destroyVm
        final List<Cloudlet> affectedExecCloudlets = resetCloudlets(vm.getCloudletScheduler().getCloudletExecList());
        final List<Cloudlet> affectedWaitingGloudlets = resetCloudlets(vm.getCloudletScheduler().getCloudletWaitingList());
        final List<Cloudlet> affectedCloudlets = Stream.concat(affectedExecCloudlets.stream(), affectedWaitingGloudlets.stream()).collect(Collectors.toList());
        vm.getHost().destroyVm(vm);
        vm.getCloudletScheduler().clear();
        // replaces broker.destroyVm

        logger.debug("Killing VM: "
                + vm.getId()
                + " to reschedule cloudlets: "
                + affectedCloudlets.size()
                + " type: "
                + vmSize);
        if(affectedCloudlets.size() > 0) {
            rescheduleCloudlets(affectedCloudlets);
        }
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

        long brokerStart = System.nanoTime();
        submitCloudletsList(affectedCloudlets);
        long brokerStop = System.nanoTime();

        double brokerTime = (brokerStop - brokerStart) / 1_000_000_000d;
        logger.debug("Rescheduling " + affectedCloudlets.size() + " cloudlets took (s): " + brokerTime);
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
        return vmCost.getVMCostPerIteration(this.clock());
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

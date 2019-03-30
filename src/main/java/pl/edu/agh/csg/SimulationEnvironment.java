package pl.edu.agh.csg;

import com.google.gson.Gson;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.StatUtils;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.DatacenterBrokerSimple2;
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
import org.cloudsimplus.listeners.EventInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

public class SimulationEnvironment {

    private static final int HISTORY_LENGTH = 30 * 60; // 30 minutes * 60s
    private static final Logger logger = LoggerFactory.getLogger(SimulationEnvironment.class.getName());

    private static final double TIME_QUANT = 1.0;

    private double vmRunningHourlyCost = 0.2;
    private long hostRam = 16 * 1024;
    private long hostBw = 50000;
    private long hostSize = 2000;
    private long hostPeMips = 10000;
    private long hostPeCnt = 4;

    private int initialVMCount = 10;
    private static final int DATACENTER_HOSTS = 1000;

    private Random random = new Random(System.currentTimeMillis());

    private CloudSim cloudSim = null;
    private DatacenterBrokerSimple2 broker = null;
    private Datacenter datacenter = null;
    private CircularFifoQueue<Double> vmCountHistory = new CircularFifoQueue<>(HISTORY_LENGTH);
    private CircularFifoQueue<Double> p99LatencyHistory = new CircularFifoQueue<>(HISTORY_LENGTH);
    private CircularFifoQueue<Double> p90LatencyHistory = new CircularFifoQueue<>(HISTORY_LENGTH);
    private CircularFifoQueue<Double> avgCPUUtilizationHistory = new CircularFifoQueue<>(HISTORY_LENGTH);
    private CircularFifoQueue<Double> p90CPUUtilizationHistory = new CircularFifoQueue<>(HISTORY_LENGTH);
    private CircularFifoQueue<Double> totalLatencyHistory = new CircularFifoQueue<>(HISTORY_LENGTH);
    private CircularFifoQueue<Double> waitingQueueLengthHistory = new CircularFifoQueue<>(HISTORY_LENGTH);
    private Double[] doubles = new Double[0];
    private int nextVmId = 0;
    private List<Cloudlet> jobs = new LinkedList<>();
    private double queueWaitPenalty = 0.00001;
    private final String testFile;
    private double until = 0.01;
    private Gson gson = new Gson();

    public SimulationEnvironment() throws IOException, InterruptedException {
        this(null);
    }

    public SimulationEnvironment(String testFile) throws IOException, InterruptedException {
        this.testFile = testFile != null ? testFile : withDefault("TEST_FILE", "KTH-SP2-1996-2.1-cln.swf");
    }

    public ResetResult reset() throws IOException, InterruptedException {
        logger.debug("Environment reset started");

        close();
        clearMetricsHistory();

        vmRunningHourlyCost = Double.parseDouble(withDefault("VM_RUNNING_HOURLY_COST", "0.2"));
        hostPeMips = Long.parseLong(withDefault("HOST_PE_MIPS", "10000"));
        hostBw = Long.parseLong(withDefault("HOST_BW", "50000"));
        hostRam = Long.parseLong(withDefault("HOST_RAM", "16384"));
        hostSize = Long.parseLong(withDefault("HOST_SIZE", "2000"));
        hostPeCnt = Long.parseLong(withDefault("HOST_PE_CNT", "4"));
        initialVMCount = Integer.parseInt(withDefault("INITIAL_VM_COUNT", "10"));
        queueWaitPenalty = Double.parseDouble(withDefault("QUEUE_WAIT_PENALTY", "0.00001"));

        cloudSim = createSimulation();
        broker = createDatacenterBroker();
        datacenter = createDatacenter();
        broker.submitVmList(createVmList());

        jobs = loadJobs();
        broker.submitCloudletList(jobs);

        // let simulation setup stuff
        // start has to be AFTER loading jobs
        cloudSim.start();
        cloudSim.runStep(0.01);
        until = TIME_QUANT;

        logger.debug("Environment reset finished");

        return new ResetResult(observationSnapshot());
    }

    private String withDefault(String parameterName, String defaultValue) {
        String envVariableValue = System.getenv(parameterName);

        if (envVariableValue != null) {
            return envVariableValue;
        }

        return defaultValue;
    }

    private void clearMetricsHistory() {
        this.vmCountHistory.clear();
        this.p90CPUUtilizationHistory.clear();
        this.p99LatencyHistory.clear();
        this.p90LatencyHistory.clear();
        this.avgCPUUtilizationHistory.clear();
        this.totalLatencyHistory.clear();
        this.waitingQueueLengthHistory.clear();

        fillWithZeros(this.vmCountHistory);
        fillWithZeros(this.p90CPUUtilizationHistory);
        fillWithZeros(this.p90LatencyHistory);
        fillWithZeros(this.p99LatencyHistory);
        fillWithZeros(this.avgCPUUtilizationHistory);
        fillWithZeros(this.totalLatencyHistory);
        fillWithZeros(this.waitingQueueLengthHistory);
    }

    private void fillWithZeros(Queue<Double> queue) {
        for (int i = 0; i < HISTORY_LENGTH; i++) {
            queue.add(0.0);
        }
    }

    public void seed() {
        // there is no randomness so far...
    }

    public void close() throws InterruptedException {
        logger.info("Simulation is synchronous - doing nothing");
    }

    private DatacenterBrokerSimple2 createDatacenterBroker() {
        return new DatacenterBrokerSimple2(cloudSim);
    }

    private List<? extends Vm> createVmList() {
        List<Vm> vmList = new ArrayList<>(1);

        for (int i = 0; i < initialVMCount; i++) {
            // 1 VM == 1 HOST for simplicity
            Vm vm = createVmWithId();
            vmList.add(vm);
        }

        return vmList;
    }

    private Vm createVmWithId() {
        Vm vm = new VmSimple(this.nextVmId, hostPeMips, hostPeCnt);
        this.nextVmId++;
        vm.setRam(hostRam).setBw(hostBw).setSize(hostSize).setCloudletScheduler(new CloudletSchedulerSpaceShared());
        return vm;
    }

    private List<Cloudlet> loadJobs() throws IOException {
        int mips = Integer.parseInt(withDefault("INPUT_MIPS", "120"));
        WorkloadFileReader reader = WorkloadFileReader.getInstance(testFile, mips, hostPeCnt);
        List<Cloudlet> cloudlets = reader.generateWorkload();

        Collections.sort(cloudlets, new Comparator<Cloudlet>() {
            @Override
            public int compare(Cloudlet left, Cloudlet right) {
                double comparison = left.getSubmissionDelay() - right.getSubmissionDelay();

                if (comparison < 0) {
                    return -1;
                }

                if (comparison > 0) {
                    return 1;
                }

                return 0;
            }
        });

        logger.info("Loaded: " + cloudlets.size() + " jobs");
//        for (Cloudlet cloudlet : cloudlets) {
//            logger.info("Cloudlet: " + cloudlet.getId() + " " + cloudlet.getSubmissionDelay());
//        }
        return cloudlets;
    }

    private Datacenter createDatacenter() {
        List<Host> hostList = new ArrayList<>();

        for (int i = 0; i < DATACENTER_HOSTS; i++) {
            List<Pe> peList = createPeList(hostPeCnt, hostPeMips);

            Host host =
                    new HostSimple(hostRam, hostBw, hostSize, peList)
                            .setRamProvisioner(new ResourceProvisionerSimple())
                            .setBwProvisioner(new ResourceProvisionerSimple())
                            .setVmScheduler(new VmSchedulerTimeShared());

            hostList.add(host);
        }

        return new DatacenterSimple(cloudSim, hostList, new VmAllocationPolicySimple());
    }

    public String render() {
        double[][] renderedEnv = {
                asPrimitives(this.vmCountHistory),
                asPrimitives(this.p99LatencyHistory),
                asPrimitives(this.p90LatencyHistory),
                asPrimitives(this.avgCPUUtilizationHistory),
                asPrimitives(this.p90CPUUtilizationHistory),
                asPrimitives(this.totalLatencyHistory),
                asPrimitives(this.waitingQueueLengthHistory)
        };
        return gson.toJson(renderedEnv);
    }

    private double[] asPrimitives(Queue<Double> queue) {
        return ArrayUtils.toPrimitive(queue.toArray(doubles));
    }

    public SimulationStepResult step(int action) {
        executeAction(action);

        for (Cloudlet cloudlet : jobs) {
            if (Cloudlet.Status.INEXEC.equals(cloudlet.getStatus())) {
                cloudSim.setCloudletsInExec();
            }
        }

        cloudSim.runStep(until);
        until += TIME_QUANT;

        collectMetrics();

        boolean done = !cloudSim.isRunning();
        double[] observation = observationSnapshot();
        double reward = calculateReward();

        logger.debug("Step finished (action: " + action + ")");

        return new SimulationStepResult(
                done,
                observation,
                reward
        );
    }

    private double[] observationSnapshot() {
        return new double[]{
                this.lastValue(vmCountHistory),
                this.lastValue(p99LatencyHistory),
                this.lastValue(p90LatencyHistory),
                this.lastValue(avgCPUUtilizationHistory),
                this.lastValue(p90CPUUtilizationHistory),
                this.lastValue(totalLatencyHistory),
                this.lastValue(waitingQueueLengthHistory)
        };
    }

    private double lastValue(CircularFifoQueue<Double> collection) {
        return collection.get(collection.size() - 1);
    }

    private double calculateReward() {
        // 0.00028 = 1/3600 - scale hourly cost to cost per second
        return -broker.getVmExecList().size() * vmRunningHourlyCost * 0.00028
                // this is the penalty we add for queue wait times
                - this.lastValue(waitingQueueLengthHistory) * this.queueWaitPenalty;
    }

    private void collectMetrics() {
        // vm counts
        this.vmCountHistory.add((double) broker.getVmExecList().size());

        // latency history
        double clock = cloudSim.clock();
        List<Double> waitingTimes = new ArrayList<>();
        for (Cloudlet cloudlet : jobs) {
            if (Cloudlet.Status.QUEUED.equals(cloudlet.getStatus())) {
                if (cloudlet.getSubmissionDelay() < clock) {
                    double systemEntryTime = cloudlet.getSubmissionDelay();
                    // systemEntryTime should be always less than exec start time
                    double realWaitingTime = clock - systemEntryTime;
                    waitingTimes.add(realWaitingTime);
                }
            }
        }
        this.waitingQueueLengthHistory.add(Double.valueOf(waitingTimes.size()));

        logger.debug("Waiting jobs: " + waitingTimes.size());

        Collections.sort(waitingTimes);
        double[] sortedWaitingTimes = ArrayUtils.toPrimitive(waitingTimes.toArray(doubles));

        p90LatencyHistory.add(percentile(sortedWaitingTimes, 0.90));
        p99LatencyHistory.add(percentile(sortedWaitingTimes, 0.99));

        // cpu usage stats
        List<Vm> input = broker.getVmExecList();
        int i = 0;
        double[] cpuPercentUsage = new double[input.size()];
        for (Vm vm : input) {
            cpuPercentUsage[i] = vm.getCpuPercentUsage();
            i++;
        }
        Arrays.sort(cpuPercentUsage);

        avgCPUUtilizationHistory.add(safeMean(cpuPercentUsage));
        p90CPUUtilizationHistory.add(percentile(cpuPercentUsage, 0.90));
        totalLatencyHistory.add(DoubleStream.of(sortedWaitingTimes).sum());
    }

    private double safeMean(double[] cpuPercentUsage) {
        if (cpuPercentUsage.length == 0) {
            return 0.0;
        }

        if (cpuPercentUsage.length == 1) {
            return cpuPercentUsage[0];
        }

        return StatUtils.mean(cpuPercentUsage);
    }

    private double percentile(double[] data, double percentile) {
        if (data.length == 0) {
            return 0.0;
        }

        if (data.length == 1) {
            return data[0];
        }

        double index = data.length * percentile;
        int roundedIndex = (int) Math.round(index) - 1;

        double retVal;
        if (index - Math.round(index) < 0.000001) {
            if (roundedIndex == data.length - 1) {
                retVal = data[roundedIndex - 1];
            } else {
                retVal = (data[roundedIndex] + data[roundedIndex + 1]) / 2.0;
            }
        } else {
            retVal = data[roundedIndex];
        }
        return retVal;
    }

    private void executeAction(int action) {
        logger.debug("Executing action: " + action);

        switch (action) {
            case 0:
                // nothing happens
                break;
            case 1:
                // adding a new vm
                Vm newVm = createVmWithId();
                broker.submitVmList(Arrays.asList(newVm));
                break;
            case 2:
                // removing randomly one of the vms
                List<Vm> vmExecList = broker.getVmExecList();
                int upperBound = vmExecList.size();

                if (upperBound > 1) {
                    int vmIdToKill = random.nextInt(upperBound);
                    Vm toDestroy = vmExecList.get(vmIdToKill);
                    ;
                    if (toDestroy != null) {
                        toDestroy.getHost().destroyVm(toDestroy);

                        vmExecList.remove(toDestroy);
                        toDestroy.getHost().destroyVm(toDestroy);
                        cloudSim.send(broker, datacenter, 0, CloudSimTags.VM_DESTROY, toDestroy);

                        Vm finalToDestroy = toDestroy;
                        List<Cloudlet> toReschedule = jobs.stream()
                                .filter(j -> j.getVm() == finalToDestroy)
                                .filter(j -> j.getStatus() != Cloudlet.Status.SUCCESS)
                                .collect(Collectors.toList());

                        logger.debug("Killing VM: to reschedule cloudlets: " + toReschedule.size());

                        toReschedule.forEach(cloudlet -> {
                            cloudlet.setStatus(Cloudlet.Status.QUEUED);
                            cloudlet.setVm(Vm.NULL);
                        });

                        broker.submitCloudletList(toReschedule);

                        logger.debug("Killing VM: " + toDestroy.getId() + " " + toDestroy.getStopTime() + " " + toDestroy.isWorking() + " ");
                    } else {
                        logger.debug("Can't kill a VM: toDestroy is NULL");
                    }
                } else {
                    logger.debug("Can't kill a VM - only one/zero running");
                }

                break;
        }

        for (Vm vm : broker.getVmExecList()) {
            logger.debug("VM is working: " + vm.getId() + " " + vm.isWorking() + " " + vm.getCpuPercentUsage() + " " + vm.getProcessor());
        }

    }

    private CloudSim createSimulation() throws IOException {
        CloudSim cloudSim = new CloudSim();
        cloudSim.addOnClockTickListener(this::onClockTickListener);
        return cloudSim;
    }

    private void onClockTickListener(EventInfo eventInfo) {
        logger.debug("onClockTickListener(): Clock tick detected: " + eventInfo.getTime());

        List<Vm> runningVms = this.broker.getVmExecList();
        if (runningVms.size() > 0) {
            int i = 0;
            for (Cloudlet cloudlet : jobs) {
                if (cloudlet.getStatus() == Cloudlet.Status.QUEUED && cloudlet.getSubmissionDelay() <= cloudSim.clock()) {
                    Vm toAssign = runningVms.get(i++ % runningVms.size());
                    while (toAssign.getCpuPercentUsage() > 0.0 && runningVms.size() > i) {
                        toAssign = runningVms.get(i++);
                    }

                    Vm currentVm = cloudlet.getVm();
                    currentVm.getCloudletScheduler().cloudletCancel(cloudlet.getId());
                    cloudlet.setVm(toAssign);
                    toAssign.getCloudletScheduler().cloudletSubmit(cloudlet);
                }
            }
        }
    }

    public long ping() {
        logger.info("pong");

        return 31415L;
    }

    private List<Pe> createPeList(long peCnt, long mips) {
        List<Pe> peList = new ArrayList<>();
        for (int i = 0; i < peCnt; i++) {
            peList.add(new PeSimple(mips, new PeProvisionerSimple()));
        }

        return peList;
    }

    public List<Cloudlet> getJobs() {
        return jobs;
    }
}
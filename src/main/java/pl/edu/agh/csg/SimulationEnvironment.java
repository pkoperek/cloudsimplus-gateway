package pl.edu.agh.csg;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.StatUtils;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
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

public class SimulationEnvironment {

    private static final int HISTORY_LENGTH = 30 * 60; // 30 minutes * 60s
    private static final Logger logger = LoggerFactory.getLogger(SimulationEnvironment.class.getName());

    private static final long HOST_RAM = 16 * 1024;
    private static final long HOST_BW = 50000;
    private static final long HOST_SIZE = 2000;
    private static final long HOST_PE_MIPS = 10000;
    private static final long HOST_PE_CNT = 4;
    private static final double VM_RUNNING_COST = 1.0;

    private static final int INITIAL_VM_COUNT = 10;
    private static final int DATACENTER_HOSTS = 100;

    private Random random = new Random(System.currentTimeMillis());

    private CloudSim cloudSim = null;
    private DatacenterBroker broker = null;
    private Datacenter datacenter = null;
    private Thread simulationThread = null;
    private double pauseAt;
    private Object simulationSemaphore = new Object();
    private CircularFifoQueue<Double> vmCountHistory = new CircularFifoQueue<>(HISTORY_LENGTH);
    private CircularFifoQueue<Double> p99LatencyHistory = new CircularFifoQueue<>(HISTORY_LENGTH);
    private CircularFifoQueue<Double> p90LatencyHistory = new CircularFifoQueue<>(HISTORY_LENGTH);
    private CircularFifoQueue<Double> avgCPUUtilizationHistory = new CircularFifoQueue<>(HISTORY_LENGTH);
    private CircularFifoQueue<Double> p90CPUUtilizationHistory = new CircularFifoQueue<>(HISTORY_LENGTH);
    private Double[] doubles = new Double[0];
    private int nextVmId = 0;
    private List<Cloudlet> jobs = new LinkedList<>();

    public SimulationEnvironment() throws IOException, InterruptedException {
        reset();
    }

    public void reset() throws IOException, InterruptedException {
        logger.debug("Environment reset started");

        close();

        clearMetricsHistory();

        pauseAt = 0.0;
        cloudSim = createSimulation();
        broker = createDatacenterBroker();
        datacenter = createDatacenter();
        broker.submitVmList(createVmList());

        jobs = loadJobs();
        broker.submitCloudletList(jobs);

        simulationThread = new Thread(new Runnable() {
            @Override
            public void run() {
                logger.debug("Starting simulation in a separate thread...");
                // pause at 0.1 to allow for processing of initial events
                cloudSim.pause(0.1);
                cloudSim.start();
                logger.debug("CloudSim simulation finished");
            }
        });
        simulationThread.start();
        logger.debug("Environment reset finished");
    }

    private void clearMetricsHistory() {
        this.vmCountHistory.clear();
        this.p90CPUUtilizationHistory.clear();
        this.p99LatencyHistory.clear();
        this.p90LatencyHistory.clear();
        this.avgCPUUtilizationHistory.clear();

        fillWithZeros(this.vmCountHistory);
        fillWithZeros(this.p90CPUUtilizationHistory);
        fillWithZeros(this.p90LatencyHistory);
        fillWithZeros(this.p99LatencyHistory);
        fillWithZeros(this.avgCPUUtilizationHistory);
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
        if (simulationThread != null) {
            logger.info("Requesting simulation abort...");
            cloudSim.abort();
            // in case the simulation is in paused state
            cloudSim.resume();
            logger.info("Waiting for simulation to end...");
            simulationThread.join();
            logger.info("Simulation stopped");
        }
    }

    private DatacenterBrokerSimple createDatacenterBroker() {
        return new DatacenterBrokerSimple(cloudSim);
    }

    private List<? extends Vm> createVmList() {
        List<Vm> vmList = new ArrayList<>(1);

        for (int i = 0; i < INITIAL_VM_COUNT; i++) {
            // 1 VM == 1 HOST for simplicity
            Vm vm = createVmWithId();
            vmList.add(vm);
        }

        return vmList;
    }

    private Vm createVmWithId() {
        Vm vm = new VmSimple(this.nextVmId, HOST_PE_MIPS, HOST_PE_CNT);
        this.nextVmId++;
        vm.setRam(HOST_RAM).setBw(HOST_BW).setSize(HOST_SIZE)
                .setCloudletScheduler(new CloudletSchedulerSpaceShared());
        return vm;
    }

    private List<Cloudlet> loadJobs() throws IOException {
        String testFile = System.getenv("TEST_FILE");
        if(testFile == null) {
            testFile = "KTH-SP2-1996-2.1-cln_50.swf";
        }
        WorkloadFileReader reader = WorkloadFileReader.getInstance(testFile, 10000);
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
        for (Cloudlet cloudlet : cloudlets) {
            logger.info("Cloudlet: " + cloudlet.getId() + " " + cloudlet.getSubmissionDelay());
        }
        return cloudlets;
    }

    private Datacenter createDatacenter() {
        List<Host> hostList = new ArrayList<>();

        for (int i = 0; i < DATACENTER_HOSTS; i++) {
            List<Pe> peList = createPeList(HOST_PE_CNT, HOST_PE_MIPS);

            Host host =
                    new HostSimple(HOST_RAM, HOST_BW, HOST_SIZE, peList)
                            .setRamProvisioner(new ResourceProvisionerSimple())
                            .setBwProvisioner(new ResourceProvisionerSimple())
                            .setVmScheduler(new VmSchedulerTimeShared());

            hostList.add(host);
        }

        return new DatacenterSimple(cloudSim, hostList, new VmAllocationPolicySimple());
    }

    public double[][] render() {
        return new double[][]{
                asPrimitives(this.vmCountHistory),
                asPrimitives(this.p99LatencyHistory),
                asPrimitives(this.p90LatencyHistory),
                asPrimitives(this.avgCPUUtilizationHistory),
                asPrimitives(this.p90CPUUtilizationHistory)
        };
    }

    private double[] asPrimitives(Queue<Double> queue) {
        return ArrayUtils.toPrimitive(queue.toArray(doubles));
    }

    public SimulationStepResult step(int action) {
        executeAction(action);
        unlockEnvironment();
        waitForStepFinish();

        collectMetrics();

        boolean done = !cloudSim.isRunning();
        double[] observation = new double[]{
                vmCountHistory.get(vmCountHistory.size() - 1),
                p99LatencyHistory.get(p99LatencyHistory.size() - 1),
                p90LatencyHistory.get(p90LatencyHistory.size() - 1),
                avgCPUUtilizationHistory.get(avgCPUUtilizationHistory.size() - 1),
                p90CPUUtilizationHistory.get(p90CPUUtilizationHistory.size() - 1)
        };
        double reward = calculateReward();

        logger.debug("Step finished (action: " + action +")");

        return new SimulationStepResult(
                done,
                observation,
                reward
        );
    }

    private double calculateReward() {
        // 1.0 stands for the amount of time of a step
        return -broker.getVmExecList().size() * VM_RUNNING_COST * 1.0;
    }

    private void collectMetrics() {
        // vm counts
        this.vmCountHistory.add((double) broker.getVmExecList().size());

        // latency history
        double cutOffTime = Math.floor(cloudSim.clock() - 1.0);
        List<Double> waitingTimes = new ArrayList<>();
        for (Cloudlet cloudlet : broker.getCloudletFinishedList()) {
            if (cloudlet.getFinishTime() > cutOffTime) {
                waitingTimes.add(cloudlet.getWaitingTime());
            }
        }

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

        avgCPUUtilizationHistory.add(StatUtils.mean(cpuPercentUsage));
        p90CPUUtilizationHistory.add(percentile(cpuPercentUsage, 0.90));
    }

    private double percentile(double[] data, double percentile) {
        String dataAsString = Arrays.toString(data);

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

    private void waitForStepFinish() {
        while (!cloudSim.isPaused() && cloudSim.isRunning()) {
            try {
                logger.debug("Waiting for simulation step to finish");
                synchronized (simulationSemaphore) {
                    simulationSemaphore.wait(100);
                }
            } catch (InterruptedException e) {
            }
        }
    }

    private void unlockEnvironment() {
        pauseAt += 1.0;
        logger.debug("step() - resuming operation at tick: " + cloudSim.clock() + " will pause at: " + pauseAt);
        cloudSim.resume();
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

                if (upperBound != 0) {
                    int vmIdToKill = random.nextInt(upperBound);
                    Vm toDestroy = null;
                    for (int i = 0; i < vmExecList.size(); i++) {
                        if (i == vmIdToKill) {
                            toDestroy = vmExecList.get(i);
                        }
                    }
                    if (toDestroy != null) {
                        toDestroy.getHost().destroyVm(toDestroy);

                        vmExecList.remove(toDestroy);
                        cloudSim.send(broker, datacenter, 0, CloudSimTags.VM_DESTROY, toDestroy);

                        logger.debug("Killing VM: " + toDestroy.getId() + " " + toDestroy.getStopTime() + " " + toDestroy.isWorking() + " ");
                    } else {
                        logger.debug("Can't kill a VM: toDestroy is NULL");
                    }
                } else {
                    logger.debug("Can't kill a VM - none running");
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
        Set<Cloudlet> cloudlets = this.broker.getCloudletCreatedList();
        int i = 0;
        for (Cloudlet cloudlet : cloudlets) {
            if (cloudlet.getStatus() == Cloudlet.Status.INSTANTIATED) {
                cloudlet.setVm(runningVms.get(i++ % runningVms.size()));
            }
        }

        if (pauseAt <= eventInfo.getTime()) {
            logger.debug("onClockTickListener(): pausing: " + pauseAt + " <= " + eventInfo.getTime());
            cloudSim.pause();
            synchronized (simulationSemaphore) {
                simulationSemaphore.notifyAll();
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

}


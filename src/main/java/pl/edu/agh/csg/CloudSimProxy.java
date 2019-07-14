package pl.edu.agh.csg;

import org.apache.commons.lang3.ArrayUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CloudSimProxy {

    private static final Logger logger = LoggerFactory.getLogger(CloudSimProxy.class.getName());
    private static final Double[] double_arr = new Double[0];
    private final DatacenterBroker broker;
    private final CloudSim cloudSim;
    private final SimulationSettings settings;
    private final VmCost vmCost;
    private final Datacenter datacenter;
    private int nextVmId;
    private final Map<Long, Double> originalSubmissionDelay = new HashMap<>();
    private final Random random = new Random(System.currentTimeMillis());

    public CloudSimProxy(SimulationSettings settings, int initialVmCount, List<Cloudlet> jobs) {
        this.settings = settings;
        this.cloudSim = new CloudSim(0.1);
        this.broker = createDatacenterBroker();
        this.datacenter = createDatacenter();
        this.vmCost = new VmCost(settings.getVmRunningHourlyCost());

        this.nextVmId = 0;
        final List<? extends Vm> vmList = createVmList(initialVmCount);
        broker.submitVmList(vmList);

        jobs.forEach(c -> originalSubmissionDelay.put(c.getId(), c.getSubmissionDelay()));
        broker.submitCloudletList(jobs);

        this.cloudSim.startSync();
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

        final DatacenterSimple datacenterSimple = new DatacenterSimple(cloudSim, hostList, new VmAllocationPolicySimple());
        return datacenterSimple;
    }

    private List<? extends Vm> createVmList(int vmCount) {
        List<Vm> vmList = new ArrayList<>(1);

        for (int i = 0; i < vmCount; i++) {
            // 1 VM == 1 HOST for simplicity
            vmList.add(createVmWithId());
        }

        return vmList;
    }

    private Vm createVmWithId() {
        Vm vm = new VmSimple(this.nextVmId, settings.getHostPeMips(), settings.getHostPeCnt());
        this.nextVmId++;
        vm
                .setRam(settings.getHostRam())
                .setBw(settings.getHostBw())
                .setSize(settings.getHostSize())
                .setCloudletScheduler(new CloudletScheduler());
        vmCost.notifyCreateVM(vm, this.cloudSim.clock());
        return vm;
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
        int i = 0;

        double adjustedInterval = interval;
        while(this.cloudSim.runFor(adjustedInterval) < target) {
            adjustedInterval = target - this.cloudSim.clock();
            adjustedInterval = adjustedInterval <= 0 ? cloudSim.getMinTimeBetweenEvents() : adjustedInterval;

            // Force stop if something runs out of control
            if(i >= 1000) {
                throw new RuntimeException("Breaking a really long loop in runFor!");
            }
            i++;
        }
    }

    public boolean isRunning() {
        return cloudSim.isRunning();
    }

    public double getNumberOfActiveVMs() {
        return (double) broker.getVmExecList().size();
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

    public double[] getWaitTimesFromLastInterval() {
        double cutOffTime = Math.floor(cloudSim.clock() - 1.0);
        List<Double> waitingTimes = new ArrayList<>();
        for (Cloudlet cloudlet : broker.getCloudletFinishedList()) {
            if (cloudlet.getFinishTime() > cutOffTime) {
                double systemEntryTime = this.originalSubmissionDelay.get(cloudlet.getId());
                // systemEntryTime should be always less than exec start time
                double realWaitingTime = cloudlet.getExecStartTime() - systemEntryTime;
                waitingTimes.add(realWaitingTime);
            }
        }

        return ArrayUtils.toPrimitive(waitingTimes.toArray(double_arr));
    }

    public void addNewVM() {
        // assuming average delay up to 97s as in 10.1109/CLOUD.2012.103
        // from anecdotal exp the startup time can be as fast as 45s
        Vm newVm = createVmWithId();
        double delay = this.cloudSim.clock() + 45 + Math.random() * 52;
        newVm.setSubmissionDelay(delay);

        broker.submitVm(newVm);
        logger.debug("VM creating requested, delay: " + delay);
    }

    public void removeRandomlyVM() {
        List<Vm> vmExecList = broker.getVmExecList();
        int upperBound = vmExecList.size();

        if (upperBound > 1) {
            int vmToKillIdx = random.nextInt(upperBound);

            final Vm vm = vmExecList.get(vmToKillIdx);
            final List<Cloudlet> affectedCloudlets = this.broker.destroyVm(vm);

            logger.debug("Killing VM: " + vm.getId() + " to reschedule cloudlets: " + affectedCloudlets.size());

            final double currentClock = cloudSim.clock();

            affectedCloudlets.forEach(cloudlet -> {
                Double submissionDelay = originalSubmissionDelay.get(cloudlet.getId());

                if (submissionDelay == null) {
                    throw new RuntimeException("Cloudlet with ID: " + cloudlet.getId() + " not seen previously! Original submission time unknown!");
                }

                if (submissionDelay < currentClock) {
                    submissionDelay = currentClock + 1.0;
                }

                cloudlet.setSubmissionDelay(submissionDelay);
            });

            broker.submitCloudletList(affectedCloudlets);
        } else {
            logger.debug("Can't kill a VM - only one running");
        }
    }

    public double clock() {
        return this.cloudSim.clock();
    }

    class CloudletScheduler extends CloudletSchedulerSpaceShared {
        @Override
        public double updateProcessing(double currentTime, List<Double> mipsShare) {
            final int sizeBefore = this.getCloudletWaitingList().size();
            final double nextSimulationTime = super.updateProcessing(currentTime, mipsShare);
            final int sizeAfter = this.getCloudletWaitingList().size();

            // if we have a new cloudlet being processed, schedule another recalculation, which should trigger a proper
            // estimation of end time
            if(sizeAfter != sizeBefore && Double.MAX_VALUE == nextSimulationTime) {
                return this.getVm().getSimulation().getMinTimeBetweenEvents();
            }

            return nextSimulationTime;
        }
    }
}

package pl.edu.agh.csg;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
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
import org.cloudbus.cloudsim.util.WorkloadFileReader;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.listeners.EventInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimulationEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(SimulationEnvironment.class.getName());

    private static final long HOST_RAM = 16 * 1024;
    private static final long HOST_BW = 50000;
    private static final long HOST_SIZE = 2000;
    private static final long HOST_PE_MIPS = 10000;
    private static final long HOST_PE_CNT = 4;

    private static final int DATACENTER_HOSTS = 100;

    private CloudSim cloudSim = null;
    private DatacenterBroker broker = null;
    private Datacenter datacenter = null;
    private Thread simulationThread = null;
    private double pauseAt;

    public SimulationEnvironment() throws IOException, InterruptedException {
        reset();
    }

    public void reset() throws IOException, InterruptedException {
        if (simulationThread != null) {
            logger.info("Requesting simulation abort...");
            cloudSim.abort();
            logger.info("Waiting for simulation to end...");
            simulationThread.join();
            logger.info("Simulation stopped");
        }

        pauseAt = 0.0;
        cloudSim = createSimulation();
        broker = createDatacenterBroker();
        datacenter = createDatacenter();
        broker.submitVmList(createVmList());

        List<Cloudlet> jobs = loadJobs();
        broker.submitCloudletList(jobs);

        simulationThread = new Thread(new Runnable() {
            @Override
            public void run() {
                logger.debug("Starting simulation in a separate thread...");
                cloudSim.pause(0.0);
                cloudSim.start();
                logger.debug("CloudSim simulation finished");
            }
        });
        simulationThread.start();
    }

    private DatacenterBrokerSimple createDatacenterBroker() {
        return new DatacenterBrokerSimple(cloudSim);
    }

    private List<? extends Vm> createVmList() {
        List<Vm> vmList = new ArrayList<>(1);

        for (int i = 0; i < DATACENTER_HOSTS; i++) {
            // 1 VM == 1 HOST for simplicity
            Vm vm = new VmSimple(i, HOST_PE_MIPS, HOST_PE_CNT);
            vm.setRam(HOST_RAM).setBw(HOST_BW).setSize(HOST_SIZE)
                    .setCloudletScheduler(new CloudletSchedulerSpaceShared());
            vmList.add(vm);
        }

        return vmList;
    }

    private List<Cloudlet> loadJobs() throws IOException {
        WorkloadFileReader reader = WorkloadFileReader.getInstance("NASA-iPSC-1993-3.1-cln.swf", 10000);
        List<Cloudlet> cloudlets = reader.generateWorkload();
        logger.info("Loaded: " + cloudlets.size() + " jobs");
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

    public void step() {
        pauseAt += 1.0;
        logger.debug("step() - resuming operation at tick: " + cloudSim.clock() + " will pause at: " + pauseAt);
        cloudSim.resume();
    }

    private CloudSim createSimulation() throws IOException {
        CloudSim cloudSim = new CloudSim();
        cloudSim.addOnClockTickListener(this::onClockTickListener);
        return cloudSim;
    }

    private void onClockTickListener(EventInfo eventInfo) {
        logger.debug("onClockTickListener(): Clock tick detected: " + eventInfo.getTime());
        if(pauseAt <= eventInfo.getTime()) {
            logger.debug("onClockTickListener(): pausing: " + pauseAt + " <= " + eventInfo.getTime() );
            cloudSim.pause();
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


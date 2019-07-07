package pl.edu.agh.csg;

import static pl.edu.agh.csg.Defaults.withDefault;

public class SimulationSettings {

    private final double vmRunningHourlyCost;
    private final long hostPeMips;
    private final long hostBw;
    private final long hostRam;
    private final long hostSize;
    private final long hostPeCnt;
    private final int defaultInitialVmCount;
    private final double queueWaitPenalty;
    private final long datacenterHostsCnt;

    public SimulationSettings() {
        vmRunningHourlyCost = Double.parseDouble(withDefault("VM_RUNNING_HOURLY_COST", "0.2"));
        hostPeMips = Long.parseLong(withDefault("HOST_PE_MIPS", "10000"));
        hostBw = Long.parseLong(withDefault("HOST_BW", "50000"));
        hostRam = Long.parseLong(withDefault("HOST_RAM", "16384"));
        hostSize = Long.parseLong(withDefault("HOST_SIZE", "2000"));
        hostPeCnt = Long.parseLong(withDefault("HOST_PE_CNT", "4"));
        defaultInitialVmCount = Integer.parseInt(withDefault("INITIAL_VM_COUNT", "10"));
        queueWaitPenalty = Double.parseDouble(withDefault("QUEUE_WAIT_PENALTY", "0.00001"));
        datacenterHostsCnt = Long.parseLong(withDefault("DATACENTER_HOSTS_CNT", "1000"));
    }

    public double getVmRunningHourlyCost() {
        return vmRunningHourlyCost;
    }

    public long getHostPeMips() {
        return hostPeMips;
    }

    public long getHostBw() {
        return hostBw;
    }

    public long getHostRam() {
        return hostRam;
    }

    public long getHostSize() {
        return hostSize;
    }

    public long getHostPeCnt() {
        return hostPeCnt;
    }

    public int getDefaultInitialVmCount() {
        return defaultInitialVmCount;
    }

    public double getQueueWaitPenalty() {
        return queueWaitPenalty;
    }

    public long getDatacenterHostsCnt() {
        return datacenterHostsCnt;
    }
}

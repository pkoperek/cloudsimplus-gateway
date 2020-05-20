package pl.edu.agh.csg;

import static pl.edu.agh.csg.Defaults.withDefault;

public class SimulationSettings {

    private final double vmRunningHourlyCost;
    private final long hostPeMips;
    private final long hostBw;
    private final long hostRam;
    private final long hostSize;
    private final int hostPeCnt;
    private final int defaultInitialVmCount;
    private final double queueWaitPenalty;
    private final long datacenterHostsCnt;
    private final long basicVmRam;
    private final long basicVmPeCount;

    public SimulationSettings() {
        // Host size is big enough to host a m5a.2xlarge VM
        vmRunningHourlyCost = Double.parseDouble(withDefault("VM_RUNNING_HOURLY_COST", "0.2"));
        hostPeMips = Long.parseLong(withDefault("HOST_PE_MIPS", "10000"));
        hostBw = Long.parseLong(withDefault("HOST_BW", "50000"));
        hostRam = Long.parseLong(withDefault("HOST_RAM", "32768"));
        hostSize = Long.parseLong(withDefault("HOST_SIZE", "16000"));
        hostPeCnt = Integer.parseInt(withDefault("HOST_PE_CNT", "8"));
        defaultInitialVmCount = Integer.parseInt(withDefault("INITIAL_VM_COUNT", "10"));
        queueWaitPenalty = Double.parseDouble(withDefault("QUEUE_WAIT_PENALTY", "0.00001"));
        datacenterHostsCnt = Long.parseLong(withDefault("DATACENTER_HOSTS_CNT", "3000"));
        basicVmRam = Long.parseLong(withDefault("BASIC_VM_RAM", "8192"));
        basicVmPeCount = Long.parseLong(withDefault("BASIC_VM_PE_CNT", "2"));
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

    public int getHostPeCnt() {
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

    public long getDatacenterCores() {
        return getDatacenterHostsCnt() * hostPeCnt;
    }

    public long getBasicVmPeCnt() {
        return this.basicVmPeCount;
    }

    public long getBasicVmSize() {
        return this.getHostSize() / 4;
    }

    public long getBasicVmBw() {
        return this.getHostBw() / 4;
    }

    public long getBasicVmRam() {
        return this.basicVmRam;
    }
}

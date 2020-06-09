package pl.edu.agh.csg;

import java.util.HashMap;
import java.util.Map;

public class VmCounter {
    private final long maxVmsPerSize;
    private final Map<String, Long> vmCounts = new HashMap<>();

    public VmCounter(long maxVmsPerSize) {
        this.maxVmsPerSize = maxVmsPerSize;
    }

    public boolean hasCapacity(String type) {
        return getCurrentOfType(type) < maxVmsPerSize;
    }

    public void recordNewVM(String type) {
        final Long current = getCurrentOfType(type);
        this.vmCounts.put(type, current + 1);
    }

    private Long getCurrentOfType(String type) {
        return this.vmCounts.getOrDefault(type, 0L);
    }

    public void initializeCapacity(String type, long initialVmsCount) {
        this.vmCounts.put(type, initialVmsCount);
    }

    public void recordRemovedVM(String type) {
        final Long currentOfType = getCurrentOfType(type);
        this.vmCounts.put(type, currentOfType - 1);
    }

    public long getStartedVms(String type) {
        return getCurrentOfType(type);
    }
}

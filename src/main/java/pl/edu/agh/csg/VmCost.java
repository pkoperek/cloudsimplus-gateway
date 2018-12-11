package pl.edu.agh.csg;

import org.cloudbus.cloudsim.vms.Vm;

import java.util.ArrayList;
import java.util.List;

public class VmCost {

    private final double SECONDS_IN_HOUR = 60 * 60;
    private final double perSecondVMCost;
    private List<Vm> createdVms = new ArrayList<>();
    private List<Vm> removedVms = new ArrayList<>();

    private final double perHourVMCost;

    public VmCost(double perHourVMCost) {
        this.perHourVMCost = perHourVMCost;
        this.perSecondVMCost = perHourVMCost * 0.00028;
    }

    public void notifyCreateVM(Vm vm, double clock) {
        this.createdVms.add(vm);
    }

    public void clear() {
        createdVms.clear();
    }

    public double getVMCostPerSecond(double clock) {
        double totalCost = 0.0;
        List<Vm> toRemove = new ArrayList<>();
        for(Vm vm : createdVms) {
            // check if the vm is started
            if(vm.getStartTime() > 0.0) {
                if(vm.getStopTime() > 0.0) {
                    // vm was stopped - we continue to pay for it within the last running hour
                    if(clock <= vm.getStopTime() + SECONDS_IN_HOUR) {
                        totalCost += perSecondVMCost;
                    } else {
                        toRemove.add(vm);
                    }
                } else {
                    // vm still running - just
                    totalCost += perSecondVMCost;
                }
            }
        }
        removedVms.addAll(toRemove);
        createdVms.removeAll(toRemove);
        return totalCost;
    }

    public double getPerHourVMCost() {
        return perHourVMCost;
    }
}

package pl.edu.agh.csg;

import org.cloudbus.cloudsim.vms.Vm;

import java.util.ArrayList;
import java.util.List;

public class VmCost {

    private final double secondsInHour;
    private final double perSecondVMCost;
    private final double speedUp;
    private List<Vm> createdVms = new ArrayList<>();
    private List<Vm> removedVms = new ArrayList<>();

    private final double perHourVMCost;

    public VmCost(double perHourVMCost, double speedUp) {
        this.perHourVMCost = perHourVMCost * speedUp;
        this.perSecondVMCost = perHourVMCost * 0.00028; // 1/3600
        this.speedUp = speedUp;
        this.secondsInHour = 60 * 60 / this.speedUp;
    }

    public void notifyCreateVM(Vm vm) {
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
            if(vm.getStartTime() > -1) {
                if(vm.getStopTime() > -1) {
                    // vm was stopped - we continue to pay for it within the last running hour
                    if(clock <= vm.getStopTime() + secondsInHour) {
                        totalCost += perSecondVMCost;
                    } else {
                        toRemove.add(vm);
                    }
                } else {
                    // vm still running - just
                    totalCost += perSecondVMCost;
                }
            } else {
                // created - not running yet, need to pay for it
                totalCost += perSecondVMCost;
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

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
    private boolean payForFullHour;

    public VmCost(double perHourVMCost, double speedUp, boolean payForFullHour) {
        this.payForFullHour = payForFullHour;
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
            double m = getSizeMultiplier(vm);
            if(vm.getStartTime() > -1) {
                if(vm.getStopTime() > -1) {
                    // vm was stopped - we continue to pay for it within the last running hour if need to
                    if(payForFullHour && (clock <= vm.getStopTime() + secondsInHour)) {
                        totalCost += perSecondVMCost * m;
                    } else {
                        toRemove.add(vm);
                    }
                } else {
                    // vm still running - just
                    totalCost += perSecondVMCost * m;
                }
            } else {
                // created - not running yet, need to pay for it
                totalCost += perSecondVMCost * m;
            }
        }
        removedVms.addAll(toRemove);
        createdVms.removeAll(toRemove);
        return totalCost;
    }

    private double getSizeMultiplier(Vm vm) {
        if("M".equals(vm.getDescription())) {
            return 2.0;
        }
        if("L".equals(vm.getDescription())) {
            return 4.0;
        }
        return 1.0;
    }

    public double getPerHourVMCost() {
        return perHourVMCost;
    }
}

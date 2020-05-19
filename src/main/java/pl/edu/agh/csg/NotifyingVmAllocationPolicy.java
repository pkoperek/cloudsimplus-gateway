package pl.edu.agh.csg;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.ArrayList;
import java.util.List;

public class NotifyingVmAllocationPolicy extends VmAllocationPolicySimple {

    private List<VmCreatedListener> listeners = new ArrayList<>();

    public void addVmCreatedListener(VmCreatedListener vmCreatedListener) {
        this.listeners.add(vmCreatedListener);
    }

    private void notifyVmCreated(String type) {
        for(VmCreatedListener listener : this.listeners) {
            listener.notifyVmCreated(type);
        }
    }

    @Override
    public boolean allocateHostForVm(Vm vm, Host host) {
        final boolean allocated = super.allocateHostForVm(vm, host);
        if(allocated) {
            notifyVmCreated(vm.getDescription());
        }
        return allocated;
    }
}

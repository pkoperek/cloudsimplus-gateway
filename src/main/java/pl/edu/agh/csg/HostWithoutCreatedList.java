package pl.edu.agh.csg;

import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.provisioners.ResourceProvisioner;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.List;

public class HostWithoutCreatedList extends HostSimple {
    public HostWithoutCreatedList(List<Pe> peList) {
        super(peList);
    }

    public HostWithoutCreatedList(List<Pe> peList, boolean activate) {
        super(peList, activate);
    }

    public HostWithoutCreatedList(ResourceProvisioner ramProvisioner, ResourceProvisioner bwProvisioner, long storage, List<Pe> peList) {
        super(ramProvisioner, bwProvisioner, storage, peList);
    }

    public HostWithoutCreatedList(long ram, long bw, long storage, List<Pe> peList) {
        super(ram, bw, storage, peList);
    }

    public HostWithoutCreatedList(long ram, long bw, long storage, List<Pe> peList, boolean activate) {
        super(ram, bw, storage, peList, activate);
    }

    @Override
    protected void addVmToCreatedList(Vm vm) {
        // do nothing to avoid accumulating vm data
    }
}

package pl.edu.agh.csg;

import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.vms.Vm;

/**
 * Fixed version of the original class - uses list of currently executable VMs
 * instead of created ones (what makes the cloudlets go puff)
 */
public class DatacenterBrokerFirstFitFixed extends DatacenterBrokerSimple {
    /**
     * The index of the last Vm used to place a Cloudlet.
     */
    private int lastVmIndex;

    /**
     * Creates a DatacenterBroker object.
     *
     * @param simulation The CloudSim instance that represents the simulation the Entity is related to
     */
    public DatacenterBrokerFirstFitFixed(final CloudSim simulation) {
        super(simulation);
    }

    /**
     * Selects the first VM with the lowest number of PEs that is able to run a given Cloudlet.
     * In case the algorithm can't find such a VM, it uses the
     * default DatacenterBroker VM mapper as a fallback.
     *
     * @param cloudlet the Cloudlet to find a VM to run it
     * @return the VM selected for the Cloudlet or {@link Vm#NULL} if no suitable VM was found
     */
    @Override
    public Vm defaultVmMapper(final Cloudlet cloudlet) {
        if (cloudlet.isBoundToVm()) {
            return cloudlet.getVm();
        }

        // if we delete a VM when in the previous iteration we had lastVmIndex set
        // to size() - 1 then we are going to explode... if we don't have the
        // line below :)
        lastVmIndex %= getVmExecList().size();

        /* The for loop just defines the maximum number of Hosts to try.
         * When a suitable Host is found, the method returns immediately. */
        final int maxTries = getVmExecList().size();
        for (int i = 0; i < maxTries; i++) {
            final Vm vm = getVmExecList().get(lastVmIndex);
            if (vm.getExpectedFreePesNumber() >= cloudlet.getNumberOfPes()) {
                LOGGER.trace("{}: {}: {} (PEs: {}) mapped to {} (available PEs: {}, tot PEs: {})",
                    getSimulation().clockStr(), getName(), cloudlet, cloudlet.getNumberOfPes(), vm,
                    vm.getExpectedFreePesNumber(), vm.getFreePesNumber());
                return vm;
            }

            /* If it gets here, the previous Vm doesn't have capacity to place the Cloudlet.
             * Then, moves to the next Vm.
             * If the end of the Vm list is reached, starts from the beginning,
             * until the max number of tries.*/
            lastVmIndex = ++lastVmIndex % getVmExecList().size();
        }

        LOGGER.warn("{}: {}: {} (PEs: {}) couldn't be mapped to any suitable VM.",
                getSimulation().clockStr(), getName(), cloudlet, cloudlet.getNumberOfPes());

        // if we return NULL, that VM is not created so the cloudlet lands
        // on "waiting" list
        return Vm.NULL;
    }

}

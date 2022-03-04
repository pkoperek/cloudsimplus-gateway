package pl.edu.agh.csg;

import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.events.SimEvent;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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

    @Override
    public void processEvent(SimEvent evt) {
        super.processEvent(evt);

        if(evt.getTag() == CloudSimTags.CLOUDLET_RETURN) {
            final Cloudlet cloudlet = (Cloudlet) evt.getData();
            LOGGER.debug("Cloudlet returned: " + cloudlet.getId() + "/" + cloudlet.getVm().getId() + " Scheduling more cloudlets...");
            requestDatacentersToCreateWaitingCloudlets();
        }

        if(evt.getTag() == CloudSimTags.VM_CREATE_ACK) {
            LOGGER.debug("Cleaning the vmCreatedList");
            this.getVmCreatedList().clear();
        }
    }

    @Override
    protected void requestDatacentersToCreateWaitingCloudlets() {
        final List<Cloudlet> scheduled = new LinkedList<>();
        final List<Cloudlet> cloudletWaitingList = getCloudletWaitingList();
        for (final Iterator<Cloudlet> it = cloudletWaitingList.iterator(); it.hasNext(); ) {
            final CloudletSimple cloudlet = (CloudletSimple) it.next();
            if (!cloudlet.getLastTriedDatacenter().equals(Datacenter.NULL)) {
                continue;
            }

            //selects a VM for the given Cloudlet
            Vm selectedVm = defaultVmMapper(cloudlet);
            if (selectedVm == Vm.NULL) {
                // all of our cloudlets use 1 PE. if there is no
                // VMs to support that - there is no more capacity in the
                // cluster - ergo, we can stop processing the list here
                break;
            }

            ((VmSimple) selectedVm).removeExpectedFreePesNumber(cloudlet.getNumberOfPes());

            cloudlet.setVm(selectedVm);
            send(getDatacenter(selectedVm),
                    cloudlet.getSubmissionDelay(), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
            cloudlet.setLastTriedDatacenter(getDatacenter(selectedVm));
            getCloudletCreatedList().add(cloudlet);
            scheduled.add(cloudlet);
            it.remove();
        }

        LOGGER.debug("requestDatacentersToCreateWaitingCloudlets scheduled: " + scheduled.size() + "/" + cloudletWaitingList.size());
        LOGGER.debug("Events cnt before: " + getSimulation().getNumberOfFutureEvents(simEvent -> true));
        for (Cloudlet cloudlet : scheduled) {
            final long totalLengthInMips = cloudlet.getTotalLength();
            final double peMips = cloudlet.getVm().getProcessor().getMips();
            final double lengthInSeconds = totalLengthInMips / peMips;
            final Datacenter datacenter = getDatacenter(cloudlet.getVm());
            final double eventDelay = lengthInSeconds + 1.0;
            
            LOGGER.debug("Cloudlet " + cloudlet.getId() + " scheduled. Updating in: " + eventDelay);


            this.getSimulation().send(
                    datacenter,
                    datacenter,
                    eventDelay,
                    CloudSimTags.VM_UPDATE_CLOUDLET_PROCESSING,
                    ResubmitAnchor.THE_VALUE
            );
        }
        LOGGER.debug("Events cnt after: " + getSimulation().getNumberOfFutureEvents(simEvent -> true));
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

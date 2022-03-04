package pl.edu.agh.csg;

import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletExecution;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerAbstract;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerSpaceShared;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

class OptimizedCloudletScheduler extends CloudletSchedulerSpaceShared {

    @Override
    protected double cloudletSubmitInternal(CloudletExecution cle, double fileTransferTime) {
        if(!this.getVm().isCreated()) {
            // It is possible, that we schedule a cloudlet, an event with processing
            // update is issued (tag: 16), but the VM gets killed before the event
            // is processed. In such a case the cloudlet does not get rescheduled,
            // because we don't know yet that this cloudlet should be!
            final Cloudlet cloudlet = cle.getCloudlet();
            final DatacenterBroker broker = cloudlet.getBroker();
            broker.submitCloudletList(Collections.singletonList(cloudlet.reset()));

            return -1.0;
        }

        return super.cloudletSubmitInternal(cle, fileTransferTime);
    }

    @Override
    public double updateProcessing(double currentTime, List<Double> mipsShare) {
        final int sizeBefore = this.getCloudletWaitingList().size();
        final double nextSimulationTime = super.updateProcessing(currentTime, mipsShare);
        final int sizeAfter = this.getCloudletWaitingList().size();

        // if we have a new cloudlet being processed, schedule another recalculation, which should trigger a proper
        // estimation of end time
        if (sizeAfter != sizeBefore && Double.MAX_VALUE == nextSimulationTime) {
            return this.getVm().getSimulation().getMinTimeBetweenEvents();
        }

        return nextSimulationTime;
    }

    @Override
    protected Optional<CloudletExecution> findSuitableWaitingCloudlet() {
        if (getVm().getProcessor().getAvailableResource() > 0) {
            final List<CloudletExecution> cloudletWaitingList = getCloudletWaitingList();
            for (CloudletExecution cle : cloudletWaitingList) {
                if (this.isThereEnoughFreePesForCloudlet(cle)) {
                    return Optional.of(cle);
                }
            }
        }

        return Optional.empty();
    }

    private void setPrivateField(String fieldName, Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = CloudletSchedulerAbstract.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(this, value);
    }

    private Object getPrivateFieldValue(String fieldName, Object source) throws IllegalAccessException, NoSuchFieldException {
        final Field field = CloudletSchedulerAbstract.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(source);
    }

    // It is safe to override this function: it is used only in one place - DatacenterBrokerAbstract:827
    @Override
    public void clear() {
        try {
            setPrivateField("cloudletWaitingList", new ArrayList<>());
            setPrivateField("cloudletExecList", new ArrayList<>());

            Set cloudletReturnedList = (Set) getPrivateFieldValue("cloudletReturnedList", this);
            cloudletReturnedList.clear();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}

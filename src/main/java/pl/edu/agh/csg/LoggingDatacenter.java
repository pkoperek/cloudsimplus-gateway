package pl.edu.agh.csg;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.Simulation;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LoggingDatacenter extends DatacenterSimple {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingDatacenter.class.getSimpleName());

    public LoggingDatacenter(Simulation simulation, List<? extends Host> hostList, VmAllocationPolicy vmAllocationPolicy) {
        super(simulation, hostList, vmAllocationPolicy);
    }

    @Override
    protected double updateCloudletProcessing() {
        final double retVal = super.updateCloudletProcessing();

        LOGGER.debug("updateCloudletProcessing: " + retVal + " (if equal to Double.MAX_VALUE: " + Double.MAX_VALUE + " no further processing scheduled");

        return retVal;
    }
}

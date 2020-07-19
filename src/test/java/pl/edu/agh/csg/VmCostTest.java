package pl.edu.agh.csg;

import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertThat;

public class VmCostTest {

    final int mipsCapacity = 4400;
    private VmCost vmCost;

    @Before
    public void setUp() throws Exception {
        vmCost = new VmCost(0.2, 60, false);
    }

    @Test
    public void testCostOfNoMachinesIsZero() {
        assertThat(vmCost.getVMCostPerIteration(0), equalTo(0.0));
    }

    @Test
    public void testCostFor11VMs() {
        // S
        vmCost.notifyCreateVM(createVmS());

        // 10x M
        for (int i = 0; i < 10; i++) {
            vmCost.notifyCreateVM(createVmM());
        }

        assertThat(vmCost.getVMCostPerIteration(1), closeTo(0.07, 0.001));
    }

    private Vm createVmS() {
        return new VmSimple(mipsCapacity, 2).setDescription("S");
    }

    private Vm createVmM() {
        return new VmSimple(mipsCapacity, 4).setDescription("M");
    }
}
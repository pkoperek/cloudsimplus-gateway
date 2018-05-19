package pl.edu.agh.csg;

import org.cloudbus.cloudsim.util.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class.getName());
    private static final Logger cloudSimLogger = LoggerFactory.getLogger("CLOUDSIM");

    public static void main(String[] args) throws Exception {
        Log.setOutput(new LogOutputStream(cloudSimLogger));
        SimulationEnvironment simulationEnvironment = new SimulationEnvironment();
        GatewayServer gatewayServer = new GatewayServer(simulationEnvironment);
        logger.info("Starting server: " + gatewayServer.getAddress() + " " + gatewayServer.getPort());
        gatewayServer.start();
    }
}

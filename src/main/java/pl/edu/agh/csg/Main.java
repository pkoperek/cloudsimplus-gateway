package pl.edu.agh.csg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class.getName());
    private static final SimulationEnvironment simulationEnvironment = new SimulationEnvironment();

    public static void main(String[] args) {
        GatewayServer gatewayServer = new GatewayServer(simulationEnvironment);
        logger.info("Starting server: " + gatewayServer.getAddress() + " " + gatewayServer.getPort());
        gatewayServer.start();
    }
}

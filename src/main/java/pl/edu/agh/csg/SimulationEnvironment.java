package pl.edu.agh.csg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimulationEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(SimulationEnvironment.class.getName());

    public long ping() {
        logger.info("pong");

        return 31415L;
    }
}

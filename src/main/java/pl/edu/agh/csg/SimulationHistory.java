package pl.edu.agh.csg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SimulationHistory {

    private static final Logger logger = LoggerFactory.getLogger(SimulationHistory.class.getName());
    private Map<String, List> history = new HashMap<>();

    public <T> void record(String key, T value) {
        if(!history.containsKey(key)) {
            history.put(key, new ArrayList<T>(1024));
        }

        history.get(key).add(value);
    }

    public void logHistory() {
        logger.info("Simulation History");
        history.entrySet().forEach(entry -> {
            logger.info(entry.getKey() + ": " + Arrays.toString(entry.getValue().toArray()));
        });
    }

    public void reset() {
        history.values().forEach(List::clear);
    }
}

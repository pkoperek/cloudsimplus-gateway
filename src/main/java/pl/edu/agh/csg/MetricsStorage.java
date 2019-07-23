package pl.edu.agh.csg;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.ArrayUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class MetricsStorage {

    private static final Double[] doubles = new Double[0];

    private final int historyLength;
    private final List<String> trackedMetrics;
    private Map<String, CircularFifoQueue<Double>> data = new HashMap<>();

    public MetricsStorage(int historyLength, List<String> trackedMetrics) {
        this.historyLength = historyLength;
        this.trackedMetrics = trackedMetrics;

        initializeStorage();
    }

    private void initializeStorage() {
        ensureMetricQueuesExist();
        zeroAllMetrics();
    }

    private void ensureMetricQueuesExist() {
        for(String metricName : this.trackedMetrics) {
            data.put(metricName, new CircularFifoQueue<>(this.historyLength));
        }
    }

    private void zeroAllMetrics() {
        for(String metricName : this.trackedMetrics) {
            final CircularFifoQueue<Double> metricQueue = data.get(metricName);
            metricQueue.clear();
            fillWithZeros(metricQueue);
        }
    }

    public void updateMetric(String metricName, Double value) {
        final CircularFifoQueue<Double> valuesQueue = data.get(metricName);
        if(valuesQueue == null) {
            throw new RuntimeException("Unknown metric: " + metricName);
        }

        valuesQueue.add(value);
    }

    private void fillWithZeros(Queue<Double> queue) {
        for (int i = 0; i < this.historyLength; i++) {
            queue.add(0.0);
        }
    }

    public double[] metricValuesAsPrimitives(String metricName) {
        final CircularFifoQueue<Double> queue = data.get(metricName);
        return ArrayUtils.toPrimitive(queue.toArray(doubles));
    }

    public double getLastMetricValue(String metricName) {
        final CircularFifoQueue<Double> valuesQueue = data.get(metricName);
        if(valuesQueue == null) {
            throw new RuntimeException("Unknown metric: " + metricName);
        }

        return valuesQueue.get(valuesQueue.size() - 1);
    }

    public void clear() {
        zeroAllMetrics();
    }
}

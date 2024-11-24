package benchmarks;


import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

public class MetricTrackingFlatMapFunction extends RichFlatMapFunction<TimeStampedEvent, TimeStampedEvent> {
    private transient Counter eventCounter;
    private long latencySum;

    private LongCounter latencyAccumulator;

    public MetricTrackingFlatMapFunction() {
        
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        eventCounter = getRuntimeContext().getMetricGroup().counter("eventCounter");
        latencySum = 0;

        latencyAccumulator = getRuntimeContext().getLongCounter("latency");
    }

    @Override
    public void flatMap(TimeStampedEvent event, Collector<TimeStampedEvent> out) {
        eventCounter.inc();
        event.setExitTimestamp(System.nanoTime());
        long latency = event.exitTimestamp - event.entryTimestamp;
        latencySum += latency;
    
        out.collect(event);
    }

    @Override
    public void close() throws Exception {
        latencyAccumulator.add(latencySum);
    }
}


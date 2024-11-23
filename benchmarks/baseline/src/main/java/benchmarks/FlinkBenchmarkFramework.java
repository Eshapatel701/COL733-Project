package benchmarks;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.concurrent.ThreadLocalRandom;

public class FlinkBenchmarkFramework {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE); // Every 5 seconds
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000); // Min pause between checkpoints

        int numEvents = 1000;

        if (args.length > 0) {
            try {
                numEvents = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid number of events provided. Using default: " + numEvents);
            }
        }

        DataStream<TimeStampedEvent> events = env.fromSequence(1, numEvents)
                .map(i -> new TimeStampedEvent("event-" + i, System.currentTimeMillis()))
                .returns(TypeInformation.of(TimeStampedEvent.class));

        DataStream<String> results = events.flatMap(new MetricTrackingFlatMapFunction());

        results.print();

        env.execute("Flink Benchmark Framework");
    }

    public static class TimeStampedEvent {
        public String value ;
        public long timestamp;

        public TimeStampedEvent(String value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    /**
     * Custom FlatMapFunction for tracking metrics
     */
    public static class MetricTrackingFlatMapFunction extends RichFlatMapFunction<TimeStampedEvent, String> {

        private transient Counter eventCounter;
        private transient Gauge<Long> latencyGauge;

        @Override
        public void open(Configuration parameters) {
            // Initialize metrics
            eventCounter = getRuntimeContext().getMetricGroup().counter("eventCounter");
            latencyGauge = getRuntimeContext().getMetricGroup().gauge("latencyGauge", () -> System.currentTimeMillis());
        }

        @Override
        public void flatMap(TimeStampedEvent event, Collector<String> out) {
            // Increment the event counter
            eventCounter.inc();

            // Compute latency
            long latency = System.currentTimeMillis() - event.timestamp;
            out.collect("Processed: " + event.value + ", Latency: " + latency + "ms");
        }
    }
}

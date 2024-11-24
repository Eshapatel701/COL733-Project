package benchmarks;

import java.sql.Time;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EventThroughputMeasurement {

    private static final Map<Integer, Long> throughputMap = new HashMap<>();
    private static final Map<Integer, Long> latencyMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        int parallelism = 1; // Set parallelism to 1 for single-threaded execution
        for (int numEvents = 1; numEvents <= 100000000; numEvents *= 10) {

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
            env.setParallelism(parallelism);

            DataStream<TimeStampedEvent> events = env.fromSequence(1, numEvents)
                    .map(i -> new TimeStampedEvent("event-" + i, System.nanoTime()))
                    .returns(TypeInformation.of(TimeStampedEvent.class));

            DataStream<TimeStampedEvent> results = events
                    .flatMap(new MetricTrackingFlatMapFunction())
                    .returns(TypeInformation.of(TimeStampedEvent.class));
            
            JobExecutionResult result = env.execute("Single Thread Metrics");

            // Retrieve and aggregate the metrics
            long totalRuntime = result.getNetRuntime();
            long totalThroughput = (long) ((double) numEvents * 1000 / (double) totalRuntime);
            long totalLatency = result.getAccumulatorResult("latency");
            throughputMap.put((int)Math.log10((double)numEvents), totalThroughput);
            latencyMap.put((int)Math.log10((double)numEvents), totalLatency / numEvents);
            // System.out.println("Total Throughput: " + totalThroughput + " events/sec");
            // System.out.println("Total Latency: " + totalLatency + " ns");
        }

        // Generate graphs after the job has completed
        System.out.println("Throughput: " + throughputMap);
        System.out.println("Latency: " + latencyMap);
        GraphUtils.generateIntegerGraph("Throughput vs Number of Events", throughputMap, "log10(numEvents)", "Throughput (events/sec)", "throughput_vs_numEvents.png");
        GraphUtils.generateIntegerGraph("Latency vs Number of Events", latencyMap, "log10(numEvents)", "Latency (ns)", "latency_vs_numEvents.png");
    }

}
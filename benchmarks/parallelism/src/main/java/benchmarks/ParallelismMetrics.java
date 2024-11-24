package benchmarks;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ParallelismMetrics {

    private static final Map<Integer, Long> throughputMap = new HashMap<>();
    private static final Map<Integer, Long> latencyMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        int numEvents = 100000000;

        if (args.length > 0) {
            try {
                numEvents = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Argument must be an integer");
                System.exit(1);
            }
        }

        for (int parallelism = 1; parallelism <= 10; parallelism++) {
            System.out.println("Running with parallelism: " + parallelism);

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
            env.setParallelism(parallelism);

            // Generate events with timestamps
            DataStream<TimeStampedEvent> events = env.fromSequence(1, numEvents)
                    .map(i -> new TimeStampedEvent("event-" + i, System.nanoTime()))
                    .returns(TypeInformation.of(TimeStampedEvent.class));

            DataStream<TimeStampedEvent> results = events
                    .flatMap(new MetricTrackingFlatMapFunction())
                    .returns(TypeInformation.of(TimeStampedEvent.class));
            
            // results.print();

            JobExecutionResult result = env.execute("Metrics Without Windowing - Parallelism " + parallelism);

            // Retrieve and aggregate the metrics
            long totalRuntime = result.getNetRuntime();
            long totalThroughput = (long) ((double) numEvents * 1000 / (double) totalRuntime);
            long totalLatency = result.getAccumulatorResult("latency");
            throughputMap.put(parallelism, totalThroughput);
            latencyMap.put(parallelism, totalLatency / numEvents);
            // System.out.println("Total Throughput: " + totalThroughput + " events/sec");
            // System.out.println("Total Latency: " + totalLatency + " ns");
        }

        // Generate graphs after all jobs have completed
        System.out.println("Throughput: " + throughputMap);
        System.out.println("Latency: " + latencyMap);
        GraphUtils.generateIntegerGraph("Throughput vs Parallelism", throughputMap, "Parallelism", "Throughput (events/sec)", "throughput_vs_parallelism_" + numEvents + ".png");
        GraphUtils.generateIntegerGraph("Latency vs Parallelism", latencyMap, "Parallelism", "Latency (ns)", "latency_vs_parallelism_" + numEvents + ".png");
    }
}
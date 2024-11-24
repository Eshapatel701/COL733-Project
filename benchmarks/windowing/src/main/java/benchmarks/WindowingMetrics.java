package benchmarks;

import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
// import org.apache.flink.runtime.state.filesystem.FileSystemCheckpointStorage;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WindowingMetrics {

    private static final Map<String, List<Tuple2<Long, Long>>> windowThroughputMap = new HashMap<>();
    private static final Map<String, List<Tuple2<Long, Long>>> windowLatencyMap = new HashMap<>();
    private static final Map<String, Long> throughputMap = new HashMap<>();
    private static final Map<String, Long> latencyMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        int numEvents = 100000000; // Number of events
        if (args.length > 0) {
            try {
                numEvents = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Argument must be an integer");
                System.exit(1);
            }
        }

        for (String windowingStrategy : Arrays.asList("tumbling", "sliding", "session")) {
            System.out.println("Running with windowing strategy: " + windowingStrategy);

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            // env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
            // env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
            // env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("/tmp/checkpoints"));

            DataStream<TimeStampedEvent> events = env.fromSequence(1, numEvents)
                    .map(i -> new TimeStampedEvent("event-" + i, System.nanoTime()))
                    .returns(TypeInformation.of(TimeStampedEvent.class));

            DataStream<String> results = events
                    .flatMap(new MetricTrackingFlatMapFunction())
                    .keyBy(event -> "constantKey")
                    .window(getWindow(windowingStrategy))
                    .apply((String key, TimeWindow window, Iterable<TimeStampedEvent> input, Collector<String> out) -> {
                        // Calculate throughput
                        long count = 0;
                        long latencySum = 0;
                        for (TimeStampedEvent event : input) {
                            count++;
                            latencySum += event.exitTimestamp - event.entryTimestamp;
                        }
                        long windowDuration = (window.getEnd() - window.getStart()) / 1000; // Convert to seconds
                        long throughput = count / windowDuration;
                        long latency = latencySum / count;
                        
                        // Save throughput for graphing
                        long windowStart = window.getStart();
                        windowThroughputMap.computeIfAbsent(windowingStrategy, k -> new ArrayList<>())
                                .add(Tuple2.of(windowStart, throughput));
                        windowLatencyMap.computeIfAbsent(windowingStrategy, k -> new ArrayList<>())
                                .add(Tuple2.of(windowStart, latency));
                        System.out.println("Window " + window + " processed " + count + " events with throughput " + throughput + " events/sec");

                        // Emit result for logging
                        // out.collect("Window " + windowRange + " processed " + count + " events with throughput " + throughput + " events/sec");
                    })
                    .returns(TypeInformation.of(String.class));
            
            results.print();

            JobExecutionResult result = env.execute("Metrics With Windowing - " + windowingStrategy);
            long totalRuntime = result.getNetRuntime();
            long totalThroughput = (long) ((double) numEvents * 1000 / (double) totalRuntime);
            long totalLatency = result.getAccumulatorResult("latency");
            throughputMap.put(windowingStrategy, totalThroughput);
            latencyMap.put(windowingStrategy, totalLatency / numEvents);
        }
        System.out.println(windowThroughputMap);
        System.out.println(windowLatencyMap);

        // Generate graph: Window vs Throughput for each strategy
        for (Map.Entry<String, List<Tuple2<Long, Long>>> entry : windowThroughputMap.entrySet()) {
            String strategy = entry.getKey();
            List<Tuple2<Long, Long>> windowThroughput = entry.getValue();
            Map<Long, Long> throughputData = new HashMap<>();
            Map<Long, Long> latencyData = new HashMap<>();
            for (Tuple2<Long, Long> tuple : windowThroughput) {
                throughputData.put(tuple.f0, tuple.f1); // Window range -> Throughput
            }
            for (Tuple2<Long, Long> tuple : windowLatencyMap.get(strategy)) {
                latencyData.put(tuple.f0, tuple.f1); // Window range -> Latency
            }
            GraphUtils.generateLongGraph("Throughput vs Window for " + strategy, throughputData, "Window", "Throughput (events/sec)",
                    "throughput_vs_window_" + strategy + ".png");
            GraphUtils.generateLongGraph("Latency vs Window for " + strategy, latencyData, "Window", "Latency (ns)",
                    "latency_vs_window_" + strategy + ".png");
        }

        GraphUtils.generateBarGraph("Throughput vs Windowing Strategy", throughputMap, "Windowing Strategy", "Throughput (events/sec)",
                "throughput_vs_windowing_strategy.png");
        GraphUtils.generateBarGraph("Latency vs Windowing Strategy", latencyMap, "Windowing Strategy", "Latency (ns)", 
                "latency_vs_windowing_strategy.png");
    }

    private static WindowAssigner<Object, TimeWindow> getWindow(String strategy) {
        switch (strategy) {
            case "tumbling":
                return TumblingProcessingTimeWindows.of(Time.seconds(10));
            case "sliding":
                return SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5));
            case "session":
                return ProcessingTimeSessionWindows.withGap(Time.seconds(10));
            default:
                throw new IllegalArgumentException("Invalid windowing strategy: " + strategy);
        }
    }
}

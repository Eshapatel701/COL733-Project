package benchmarks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;

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

            DataStream<TimeStampedEvent> events = env.fromSequence(1, numEvents)
                    .map(i -> new TimeStampedEvent("event-" + i, System.nanoTime()))
                    .returns(TypeInformation.of(TimeStampedEvent.class));

            DataStream<String> results = events
                    .flatMap(new MetricTrackingFlatMapFunction(parallelism))
                    .returns(TypeInformation.of(String.class));
            
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
        generateGraph("Throughput vs Parallelism", throughputMap, "Parallelism", "Throughput (events/sec)", "throughput_vs_parallelism_" + numEvents + ".png");
        generateGraph("Latency vs Parallelism", latencyMap, "Parallelism", "Latency (ns)", "latency_vs_parallelism_" + numEvents + ".png");
    }

    public static class MetricTrackingFlatMapFunction extends RichFlatMapFunction<TimeStampedEvent, String> {
        private transient Counter eventCounter;
        private final int parallelism;
        private long latencySum;

        private LongCounter latencyAccumulator;

        public MetricTrackingFlatMapFunction(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            eventCounter = getRuntimeContext().getMetricGroup().counter("eventCounter");
            latencySum = 0;

            latencyAccumulator = getRuntimeContext().getLongCounter("latency");
        }

        @Override
        public void flatMap(TimeStampedEvent event, Collector<String> out) {
            eventCounter.inc();
            long latency = System.nanoTime() - event.timestamp;
            latencySum += latency;
            // long eventCount = eventCounter.getCount();
            // if ((eventCount - 1) % 100 == 0) {
                // System.out.println("Parallelism: " + this.parallelism + " Processed: " + event.value + ", Latency: " + latency + " ns");
            // }

            // out.collect("Processed: " + event.value + ", Latency: " + latency + " ns");
        }

        @Override
        public void close() throws Exception {
            latencyAccumulator.add(latencySum);
        }
    }

    private static void generateGraph(String title, Map<Integer, Long> data, String xAxisLabel, String yAxisLabel, String fileName) {
        List<Integer> xData = new ArrayList<>(data.keySet());
        List<Long> yData = new ArrayList<>(data.values());

        XYChart chart = new XYChartBuilder().width(800).height(600).title(title).xAxisTitle(xAxisLabel).yAxisTitle(yAxisLabel).build();
        chart.addSeries(title, xData, yData);

        try {
            BitmapEncoder.saveBitmap(chart, fileName, BitmapEncoder.BitmapFormat.PNG);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
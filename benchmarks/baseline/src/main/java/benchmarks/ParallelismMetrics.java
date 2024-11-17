package benchmarks;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.knowm.xchart.*;
import org.apache.flink.api.common.JobExecutionResult;

import java.util.*;

public class ParallelismMetrics {

    private static final Map<Integer, Long> throughputMap = new HashMap<>();
    private static final Map<Integer, Long> latencyMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        int numEvents = 100000;
        if (args.length > 0) {
            try {
                numEvents = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid number of events provided. Using default: " + numEvents);
            }
        }

        for (int parallelism = 1; parallelism <= 10; parallelism++) {
            System.out.println("Running with parallelism: " + parallelism);

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
            env.setParallelism(parallelism);

            DataStream<TimeStampedEvent> events = env.fromSequence(1, numEvents)
                    .map(i -> new TimeStampedEvent("event-" + i, System.currentTimeMillis()))
                    .returns(TypeInformation.of(TimeStampedEvent.class));

            DataStream<String> results = events
                    .flatMap(new MetricTrackingFlatMapFunction(parallelism))
                    .returns(TypeInformation.of(String.class));
            
            // results.print();

            JobExecutionResult result = env.execute("Metrics Without Windowing - Parallelism " + parallelism);

            // Retrieve and aggregate the metrics
            long totalThroughput = result.getAccumulatorResult("throughput");
            long totalLatency = result.getAccumulatorResult("latency");
            throughputMap.put(parallelism, totalThroughput);
            latencyMap.put(parallelism, totalLatency / numEvents);
            System.out.println("Total Throughput: " + totalThroughput + " events/sec");
            System.out.println("Average Latency: " + totalLatency / numEvents + " ms");
        }

        // Generate graphs after all jobs have completed
        System.out.println("Throughput: " + throughputMap);
        System.out.println("Latency: " + latencyMap);
        // generateGraph("Throughput vs Parallelism", throughputMap, "Parallelism", "Throughput (events/sec)", "throughput.png");
        // generateGraph("Latency vs Parallelism", latencyMap, "Parallelism", "Latency (ms)", "latency.png");
    }

    public static class TimeStampedEvent {
        public String value;
        public long timestamp;

        public TimeStampedEvent(String value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    public static class MetricTrackingFlatMapFunction extends RichFlatMapFunction<TimeStampedEvent, String> {
        private transient Counter eventCounter;
        private final int parallelism;
        private long startTime;
        private long latencySum;

        private LongCounter throughputAccumulator;
        private LongCounter latencyAccumulator;

        public MetricTrackingFlatMapFunction(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            eventCounter = getRuntimeContext().getMetricGroup().counter("eventCounter");
            startTime = System.currentTimeMillis();
            latencySum = 0;

            throughputAccumulator = getRuntimeContext().getLongCounter("throughput");
            latencyAccumulator = getRuntimeContext().getLongCounter("latency");
        }

        @Override
        public void flatMap(TimeStampedEvent event, Collector<String> out) {
            eventCounter.inc();
            long latency = System.currentTimeMillis() - event.timestamp;
            latencySum += latency;

            out.collect("Processed: " + event.value + ", Latency: " + latency + " ms");
        }

        @Override
        public void close() throws Exception {
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            long throughput = eventCounter.getCount() * 1000 / duration;
            long avgLatency = latencySum / eventCounter.getCount();

            throughputAccumulator.add(throughput);
            latencyAccumulator.add(latencySum);

            // System.out.println("Throughput: " + throughput + " events/sec");
            // System.out.println("Average Latency: " + avgLatency + " ms");
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
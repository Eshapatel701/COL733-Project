
package benchmarks ; 

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;

import org.apache.flink.core.fs.FileSystem;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import com.codahale.metrics.SlidingWindowReservoir;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;


public class Baseline {
    public static void main(String[] args) throws Exception {
       
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a stream of TimestampedEvent
        DataStream<TimestampedEvent> events = env.fromElements(
                new TimestampedEvent("event1", System.currentTimeMillis()),
                new TimestampedEvent("event2", System.currentTimeMillis() + 1000),
                new TimestampedEvent("event3", System.currentTimeMillis() + 2000)
        );

        // Measure latency and output metrics
        events.flatMap(new LatencyHistogram()).print();

        env.execute("Baseline");
    }

    public static class TimestampedEvent {
        public String value ;
        public long timestamp;

        public TimestampedEvent(String value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    public static class LatencyHistogram extends RichFlatMapFunction<TimestampedEvent, String> {
        private transient Histogram latencyHistogram;
        // private transient Histogram histogram;

        @Override
        public void open(Configuration parameters) throws Exception {
            
            com.codahale.metrics.Histogram dropwizardHistogram =
                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
            
            latencyHistogram = getRuntimeContext().getMetricGroup()
            .histogram("eventLatencyHistogram", new DropwizardHistogramWrapper(dropwizardHistogram));
            

            // MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
            // latencyHistogram = metricGroup.histogram("eventLatency", new SimpleHistogram());
        }

        @Override
        public void flatMap(TimestampedEvent event, Collector<String> out) {
            long latency = System.currentTimeMillis() - event.timestamp;
            latencyHistogram.update(latency);
            out.collect("Processed: " + event.value + ", Latency: " + latency + " ms");
        }

        @Override
        public void close() throws Exception {
            // HistogramStatistics stats = latencyHistogram.getStatistics();
            
            
            // System.out.println("Min Latency: " + stats.getMin() + " ms");
            // System.out.println("Max Latency: " + stats.getMax() + " ms");
            // System.out.println("Mean Latency: " + stats.getMean() + " ms");
            
            // stats.getMax().writeAsText("output.txt", FileSystem.WriteMode.OVERWRITE);
            
            String output_path = "/home/baadalvm/col733-project/flink-1.20.0/benchmarks/metrics/output.txt" ; 
            try (PrintWriter writer = new PrintWriter(new FileWriter(output_path, true))) {
                writer.println("Latency Histogram Statistics:");
                writer.println("Min: " + latencyHistogram.getStatistics().getMin());
                writer.println("Max: " + latencyHistogram.getStatistics().getMax());
                writer.println("Mean: " + latencyHistogram.getStatistics().getMean());
            }
        }
    }
}



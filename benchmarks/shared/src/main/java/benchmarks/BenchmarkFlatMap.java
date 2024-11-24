package benchmarks ; 





import java.io.FileWriter;
import java.io.PrintWriter;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.util.Collector;

import com.codahale.metrics.SlidingWindowReservoir;


public class BenchmarkFlatMap extends RichFlatMapFunction<TimeStampedEvent, String> {
    // histogram to measure latency 
    private transient Histogram latencyHistogram;
    
    // counters to measure throughput
    private transient long startTime = 0 ; 
    private transient long endTime = 0 ; 
    private transient long recordCount = 0;
    
    // gauge to  measure throughput of this operator 
    private transient long lastTime = 0;
    private transient Gauge<Double> throughputGauge;
    private transient double totalThroughput = 0.0;
    private transient int throughputIntervals = 0;


    @Override
    public void open(Configuration parameters) throws Exception {
        
    

        // setup latency histogram
        com.codahale.metrics.Histogram dropwizardHistogram1 =
            new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
        
        latencyHistogram = getRuntimeContext().getMetricGroup()
        .histogram("eventLatencyHistogram", new DropwizardHistogramWrapper(dropwizardHistogram1));
        
        // setup throughput metric 
        lastTime = startTime = System.currentTimeMillis();
        getRuntimeContext().getMetricGroup().gauge("throughput", () -> calculateThroughput());

    }

    

    
    @Override
    public void flatMap(TimeStampedEvent event, Collector<String> out) {
        // track latency 
        long latency = System.currentTimeMillis() - event.entryTimestamp;
        latencyHistogram.update(latency);
        
        // track number of records for measuring throughput 
        recordCount ++ ; 
        out.collect("Processed: " + event.value + ", Latency: " + latency + " ms" + " Record Count : " + recordCount);
    }


    private double calculateThroughput() {
        // invoked by flink when gauging gauge
        long currentTime = System.currentTimeMillis();
        long elapsedMillis = currentTime - lastTime;
        if (elapsedMillis == 0) {
            return 0.0;
        }
        double throughput = recordCount * 1000.0 / elapsedMillis;
        lastTime = currentTime;

        totalThroughput += throughput; // Accumulate throughput
        throughputIntervals++; // Count the number of intervals

        recordCount = 0; // Reset record count for the next interval
        System.out.printf("currentThroughput is %f\n", throughput) ; 
        return throughput;
    }

    @Override
    public void close() throws Exception {
        
        // double averageThroughput = totalThroughput / throughputIntervals;
        endTime = System.currentTimeMillis() ; 
        double duration = (endTime - startTime)/1000.0 ; 
        double averageThroughput = recordCount / duration ; 
        
        String output_path = "output.txt" ; 
        try (PrintWriter writer = new PrintWriter(new FileWriter(output_path, false))) {
            writer.println("Latency Histogram Statistics:");
            writer.println("Min: " + latencyHistogram.getStatistics().getMin());
            writer.println("Max: " + latencyHistogram.getStatistics().getMax());
            writer.println("Mean: " + latencyHistogram.getStatistics().getMean());
            writer.println("Average Throughput: " + averageThroughput) ; 
        }


    }
}
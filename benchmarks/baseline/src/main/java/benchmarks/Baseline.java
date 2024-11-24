
package benchmarks ; 

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.IntStream;
import java.util.stream.Collectors; 

public class Baseline {
    public static void main(String[] args) throws Exception {
       
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // // Create a stream of TimeStampedEvent
        // DataStream<TimeStampedEvent> events = env.fromElements(
        //         new TimeStampedEvent("event1", System.currentTimeMillis()),
        //         new TimeStampedEvent("event2", System.currentTimeMillis()),
        //         new TimeStampedEvent("event3", System.currentTimeMillis())
        // );

        // Generate a data source 
        int numEvents = 1000000 ; 
        long sourceStartTime = System.currentTimeMillis() ; 

        DataStream<TimeStampedEvent> source = env.fromCollection(
                      IntStream.range(0, numEvents) // Generate integers from 0 to 9999
                     .mapToObj(i -> new TimeStampedEvent("record_" + i, System.currentTimeMillis() )) // Map each integer to a string
                     .collect(Collectors.toList())
        );


        long sourceEndTime = System.currentTimeMillis() ; 
        System.out.printf("Time taken to generate source is %f\n", (sourceEndTime - sourceStartTime ) / 1000.0) ; 
        // Measure latency for flatMap operator 
        source.flatMap(new BenchmarkFlatMap());

        env.execute("Baseline");
    }

    

    
}


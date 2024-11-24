
# echo "Running the Baseline job..."
# ./bin/flink run -c benchmarks.Baseline ./benchmarks/baseline/target/baseline-1.0-SNAPSHOT.jar
# echo "Running the Parallelism job..."
# ./bin/flink run -c benchmarks.ParallelismMetrics ./benchmarks/parallelism/target/parallelism-1.0-SNAPSHOT.jar
# echo "Running the Event Throughput Measurement job..."
# ./bin/flink run -c benchmarks.EventThroughputMeasurement ./benchmarks/baseline/target/baseline-1.0-SNAPSHOT.jar
echo "Running the Windowing job..."
./bin/flink run -c benchmarks.WindowingMetrics ./benchmarks/windowing/target/windowing-1.0-SNAPSHOT.jar
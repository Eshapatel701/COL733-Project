echo "Building the shared module..."
cd benchmarks/shared
mvn clean install
echo "Building the baseline module..."
cd ../baseline
mvn clean package
echo "Building the parallelism module..."
cd ../parallelism
mvn clean package
echo "Building the windowing module..."
cd ../windowing
mvn clean package
# Beam-examples
    Apache Beam example project - unbounded stream processing using Kafka
    
    
### Build
    mvn clean install

### Run the Pipeline:
    mvn compile exec:java -Dexec.mainClass=com.sunil.WindowedWordCount -Pdirect-runner -Dexec.args="--output=./output/"
    
### JSON payload for Kafka topic
    cd scripts
    python3 GenMessage.py <name> <message<optional>> <epoch time in ms<optional>>            

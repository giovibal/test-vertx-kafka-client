# Kafka Producer with vert.x


## Quick Start

### Compile

> java 11 required

    mvn -DskipTests clean package
    
### Run

    java -jar target/text-vertx-kafka-client-fat.jar
    
or with kafka servers and topic name

    java -jar target/text-vertx-kafka-client-fat.jar localhost:9092 a_topic

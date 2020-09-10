package io.github.giovibal.kafka;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Producer extends AbstractVerticle {

    public static void main(String[] args) {
        String kafkaBootstrapServers = args.length>=1 ? args[0] : "localhost:9092";
        String kafkaTopic = args.length>=2 ? args[1] : "a_topic";

        System.out.println("kafkaBootstrapServers: "+ kafkaBootstrapServers);
        System.out.println("kafkaTopic: "+ kafkaTopic);

        Producer producerVerticle = new Producer(kafkaBootstrapServers, kafkaTopic);

        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(producerVerticle);
    }

    public Producer(String kafkaBootstrapServers,
                    String kafkaTopic) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaTopic = kafkaTopic;
    }

    @Override
    public void start() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", kafkaBootstrapServers);
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        config.put("auto.offset.reset", "latest");
        config.put("enable.auto.commit", "true");
        // producer
        config.put("acks", "1");

        // kafka clients
        kafkaProducer = KafkaProducer.create(vertx, config);
        System.out.println("producer started");

        // publish
        try {
            String mqttTopic = "simple/mqtt/topic";
            byte[] payload = "{\"a\":\"messsage\"}".getBytes(StandardCharsets.UTF_8);
            Long timestamp = System.currentTimeMillis();
            Integer partition = null;
            KafkaProducerRecord<String, byte[]> record = KafkaProducerRecord.create(kafkaTopic, mqttTopic, payload, timestamp, partition);
            record.addHeader("a_header", UUID.randomUUID().toString());
            kafkaProducer.write(record);

            String val = new String(record.value(), StandardCharsets.UTF_8);
            System.out.println("message sent: "+ val);

            kafkaProducer.flush(unused -> {
                System.out.println("message published: "+ val);
                vertx.close();
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void stop() throws Exception {
        kafkaProducer.close(res -> {
            if (res.succeeded()) {
                System.out.println("producer is now closed");
            } else {
                res.cause().printStackTrace();
            }
        });
    }




    private String kafkaBootstrapServers;
    private String kafkaTopic;

    private KafkaProducer<String, byte[]> kafkaProducer;

}

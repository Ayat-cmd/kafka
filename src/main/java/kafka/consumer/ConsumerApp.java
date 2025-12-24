package kafka.consumer;

import kafka.consumer.config.KafkaConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;

public class ConsumerApp {
    public static void main(String[] args) {
        KafkaConsumerConfig consumerConfig = new KafkaConsumerConfig();
        consumerConfig.setProperties("bootstrap.servers", "broker1:9092, broker2:9092");
        consumerConfig.setProperties("group.id", "CountryCounter");
        consumerConfig.setProperties("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.setProperties("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerConfig.build());
        kafkaConsumer.subscribe(Collections.singleton("demo-partitioner"));
    }
}

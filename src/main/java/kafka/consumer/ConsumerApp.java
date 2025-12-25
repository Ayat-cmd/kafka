package kafka.consumer;

import kafka.consumer.config.KafkaConsumerConfig;
import kafka.consumer.service.ConsumerService;

import java.util.Arrays;

public class ConsumerApp {
    public static void main(String[] args) {
        KafkaConsumerConfig consumerConfig = new KafkaConsumerConfig();
        consumerConfig.setProperties("bootstrap.servers", "localhost:9092");
        consumerConfig.setProperties("group.id", "CountryCounter");
        consumerConfig.setProperties("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.setProperties("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        ConsumerService<String, String> consumerService = new ConsumerService<>(consumerConfig);
        consumerService.getMessages(Arrays.asList("demo-partitioner", "SPRING-DEMO"));
    }
}

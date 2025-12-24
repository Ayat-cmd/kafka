package kafka.consumer;

import kafka.consumer.config.KafkaConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;

public class ConsumerApp {
    public static void main(String[] args) {
        KafkaConsumerConfig consumerConfig = new KafkaConsumerConfig();
        consumerConfig.setProperties("bootstrap.servers", "localhost:9092");
        consumerConfig.setProperties("group.id", "CountryCounter");
        consumerConfig.setProperties("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.setProperties("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerConfig.build())){
            //kafkaConsumer.subscribe(Arrays.asList("demo-partitioner", "SPRING-DEMO"));
            kafkaConsumer.subscribe(Collections.singleton("demo-partitioner"));
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> message : records) {
                    System.out.println("Topic: " + message.topic() + " Partition: " + message.partition() +
                            " Offset: " + message.offset() + " Key: " + message.key() + " Value: " + message.value());
                }
            }
        }
    }
}

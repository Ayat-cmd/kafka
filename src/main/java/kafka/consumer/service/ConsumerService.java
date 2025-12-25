package kafka.consumer.service;

import kafka.consumer.config.KafkaConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;

public class ConsumerService<K, V> {
    private final KafkaConsumerConfig consumerConfig;

    public ConsumerService(KafkaConsumerConfig consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    public void getMessages(List<String> topics) {
        try (KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<>(this.consumerConfig.build())) {
            //kafkaConsumer.subscribe(Arrays.asList("demo-partitioner", "SPRING-DEMO"));
//            kafkaConsumer.subscribe(Collections.singleton("demo-partitioner"));
            kafkaConsumer.subscribe(topics);
            while (true) {
                ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<K, V> message : records) {
                    System.out.println("Topic: " + message.topic() + " Partition: " + message.partition() +
                            " Offset: " + message.offset() + " Key: " + message.key() + " Value: " + message.value());
                }
            }
        }
    }
}

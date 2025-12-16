package kafka.producer;

import kafka.producer.config.KafkaProducerConfig;
import kafka.producer.service.ProducerService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();

        kafkaProducerConfig.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProducerConfig.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerConfig.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        ProducerService producerService = new ProducerService(kafkaProducerConfig);
        producerService.asyncSendMessage("my-topic", "My first async message");
    }
}
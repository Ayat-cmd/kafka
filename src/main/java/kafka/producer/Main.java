package kafka.producer;

import kafka.producer.config.KafkaProducerConfig;
import kafka.producer.dto.Customer;
import kafka.producer.service.ProducerService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {

        Customer customer = new Customer("Ayat", 1);

        sendJsonCustomerMessageWithCustomPartitioner("demo-partitioner", customer);
    }

    private static void sendStringMessage(String topic, String message) {
        KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();

        kafkaProducerConfig.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProducerConfig.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerConfig.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        ProducerService<String, String> producerService = new ProducerService<>(kafkaProducerConfig);
        producerService.asyncSendMessage(topic, "str key", message);
    }

    private static void sendCustomerMessage(String topic, Customer customer) {
        KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();

        kafkaProducerConfig.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProducerConfig.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerConfig.setProperty("value.serializer", "kafka.producer.utils.CustomerSerializer");

        ProducerService<String, Customer> producerService = new ProducerService<>(kafkaProducerConfig);
        producerService.asyncSendMessage(topic, "cutomer key", customer);
    }

    private static void sendJsonCustomerMessage(String topic, Customer customer) {
        KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();

        kafkaProducerConfig.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProducerConfig.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerConfig.setProperty("value.serializer", "kafka.producer.utils.CustomerJsonSerializer");

        ProducerService<String, Customer> producerService = new ProducerService<>(kafkaProducerConfig);
        producerService.asyncSendMessage(topic, "cutomer key", customer);
    }

    private static void sendJsonCustomerMessageWithCustomPartitioner(String topic, Customer customer) {
        KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();

        kafkaProducerConfig.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProducerConfig.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerConfig.setProperty("value.serializer", "kafka.producer.utils.CustomerJsonSerializer");
        kafkaProducerConfig.setProperty("partitioner.class", "kafka.producer.utils.BananaPartitioner");

        ProducerService<String, Customer> producerService = new ProducerService<>(kafkaProducerConfig);
        producerService.asyncSendMessage(topic, "Banana", customer);
        producerService.asyncSendMessage(topic, "key", customer);
    }
}
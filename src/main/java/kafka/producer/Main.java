package kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();

        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps)){
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("my-topic", "key", "Hello World");
            producer.send(producerRecord);
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
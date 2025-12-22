package kafka.producer.service;

import kafka.producer.config.KafkaProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerService<K, V> {

    private final KafkaProducerConfig kafkaProducerConfig;

    public ProducerService(KafkaProducerConfig kafkaProducerConfig) {
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    public void sendMessage(String topic, K key, V message) {
        try (KafkaProducer<K, V> producer = new KafkaProducer<>(kafkaProducerConfig.build())){
            ProducerRecord<K, V> producerRecord =
                    new ProducerRecord<>(topic, key, message);
            producer.send(producerRecord);
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void syncSendMessage(String topic, K key, V message) {
        try (KafkaProducer<K, V> producer = new KafkaProducer<>(kafkaProducerConfig.build())){
            ProducerRecord<K, V> producerRecord =
                    new ProducerRecord<>(topic, key, message);
            RecordMetadata result = producer.send(producerRecord).get();
            producer.flush();
            System.out.println("Send message: " + message + ", partition: " + result.partition() + ", offset: " + result.offset());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void asyncSendMessage(String topic, K key, V message) {
        try (KafkaProducer<K, V> producer = new KafkaProducer<>(kafkaProducerConfig.build())){
            ProducerRecord<K, V> producerRecord =
                    new ProducerRecord<>(topic, key, message);
            producer.send(producerRecord, new ProducerCallbackService());
            System.out.println("Send message");
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package kafka.consumer;

import kafka.consumer.config.KafkaConsumerConfig;
import kafka.consumer.dto.Customer;
import kafka.consumer.service.ThreadListener;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerApp {
    public static void main(String[] args) {
        KafkaConsumerConfig consumerConfig = new KafkaConsumerConfig();
        consumerConfig.setProperties("bootstrap.servers", "localhost:9092");
        consumerConfig.setProperties("group.id", "group-1");
        consumerConfig.setProperties("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.setProperties("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.setProperties("enable.auto.commit", "false");

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.submit(new ThreadListener<String, String>(consumerConfig.build(), Collections.singleton("customer-json-topic")));
//        executorService.submit(new ThreadListener(consumerConfig.build(),  Collections.singleton("demo-partitioner1")));

        KafkaConsumerConfig consumerConfig2 = new KafkaConsumerConfig();
        consumerConfig2.setProperties("bootstrap.servers", "localhost:9092");
        consumerConfig2.setProperties("group.id", "group-3");
        consumerConfig2.setProperties("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig2.setProperties("value.deserializer", "kafka.consumer.utils.CustomerDeserializer");
        consumerConfig2.setProperties("auto.offset.reset", "earliest");
        consumerConfig2.setProperties("enable.auto.commit", "false");

        executorService.submit(new ThreadListener<String, Customer>(consumerConfig2.build(),  Collections.singleton("customer-topic")));

        KafkaConsumerConfig consumerConfig3 = new KafkaConsumerConfig();
        consumerConfig3.setProperties("bootstrap.servers", "localhost:9092");
        consumerConfig3.setProperties("group.id", "group-json");
        consumerConfig3.setProperties("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig3.setProperties("value.deserializer", "kafka.consumer.utils.CustomerJsonDeserializer");
        consumerConfig3.setProperties("auto.offset.reset", "earliest");
        consumerConfig3.setProperties("enable.auto.commit", "false");

        executorService.submit(
                new ThreadListener<String, Customer>(
                        consumerConfig3.build(),  Collections.singleton("customer-json-topic")));
    }
}

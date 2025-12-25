package kafka.consumer;

import kafka.consumer.config.KafkaConsumerConfig;
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

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(new ThreadListener(consumerConfig.build(), Collections.singleton("demo-partitioner1")));
        executorService.submit(new ThreadListener(consumerConfig.build(),  Collections.singleton("demo-partitioner1")));

//        KafkaConsumerConfig consumerConfig2 = new KafkaConsumerConfig();
//        consumerConfig2.setProperties("bootstrap.servers", "localhost:9092");
//        consumerConfig2.setProperties("group.id", "group-2");
//        consumerConfig2.setProperties("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        consumerConfig2.setProperties("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        executorService.submit(new ThreadListener(consumerConfig2.build(),  Collections.singleton("demo-partitioner")));
    }
}

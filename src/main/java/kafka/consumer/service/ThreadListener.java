package kafka.consumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;

public class ThreadListener extends Thread {

    private final Properties properties;
    private Collection<String> topics;

    public ThreadListener(Properties properties, Collection<String> topics) {
        this.properties = properties;
        this.topics = topics;
    }

    @Override
    public void run() {
        this.listen();
    }

    private void listen() {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.properties);
        consumer.subscribe(this.topics);

        try {
            while (true) {

                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for(ConsumerRecord<String, String> message : consumerRecords) {
                    System.out.println("Thread: " + Thread.currentThread().getName() + " Key: " + message.key() + " Value: " + message.value());
                }

                try {
                    consumer.commitAsync();
                } catch (Exception e) {
                    System.out.println("Thread: " + Thread.currentThread().getName() + " CommitAsync Exception: " + e.getMessage());
                }
            }
        } finally {
            consumer.close();
            System.out.println("Thread: " + Thread.currentThread().getName() + " Close");
        }
    }
}

package kafka.consumer.service;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ThreadListener extends Thread {

    private final Properties properties;
    private Collection<String> topics;
    private Map<TopicPartition, OffsetAndMetadata> offsets;
    private int countMessages;

    public ThreadListener(Properties properties, Collection<String> topics) {
        this.properties = properties;
        this.topics = topics;
        this.offsets = new HashMap<>();
        this.countMessages = 0;
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
                    this.offsets.put(
                            new TopicPartition(message.topic(), message.partition()),
                            new OffsetAndMetadata(message.offset())
                    );
                    this.countMessages++;
                    if (this.countMessages % 10 == 0) {
                        consumer.commitAsync(this.offsets, new OffsetCommitCallbackService());
                    }
                }

                consumer.commitAsync(new OffsetCommitCallbackService());
            }
        } finally {
            consumer.close();
            System.out.println("Thread: " + Thread.currentThread().getName() + " Close");
        }
    }
}

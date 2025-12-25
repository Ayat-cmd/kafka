package kafka.consumer.service;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

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

    public Map<TopicPartition, OffsetAndMetadata> getOffsets() {
        return offsets;
    }

    private void listen() {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.properties);

        try {
            consumer.subscribe(this.topics, new ConsumerRebalanceListenerService(consumer, this::getOffsets));

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

            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } catch (Exception e) {
            System.out.println("Unexpected error");
            e.printStackTrace();
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
                System.out.println("Thread: " + Thread.currentThread().getName() + " Close");
            }
        }
    }
}

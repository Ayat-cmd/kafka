package kafka.consumer.service;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

public class ConsumerRebalanceListenerService implements ConsumerRebalanceListener {

    private KafkaConsumer<String, String> consumer;
    private Supplier<Map<TopicPartition, OffsetAndMetadata>> offsetSupplier;

    public ConsumerRebalanceListenerService(
            KafkaConsumer<String, String> consumer,
            Supplier<Map<TopicPartition, OffsetAndMetadata>> offsetSupplier
    ) {
        this.consumer = consumer;
        this.offsetSupplier = offsetSupplier;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Lost partitions: " + partitions);
        consumer.commitSync(this.offsetSupplier.get());
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {

    }
}

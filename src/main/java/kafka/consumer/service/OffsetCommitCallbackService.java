package kafka.consumer.service;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Objects;

public class OffsetCommitCallbackService implements OffsetCommitCallback {
    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (Objects.nonNull(exception)) {
            System.out.println("Thread: " + Thread.currentThread().getName() + " Error: " + exception.getMessage() +
                    " Offset: " + offsets);
        }
    }
}

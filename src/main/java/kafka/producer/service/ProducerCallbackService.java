package kafka.producer.service;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Objects;

public class ProducerCallbackService implements Callback {

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (Objects.nonNull(e)) {
            e.printStackTrace();
        } else {
            System.out.println("partition: " + recordMetadata.partition() + ", offset: " + recordMetadata.offset());
            System.out.println("Message sent successfully");
        }
    }
}

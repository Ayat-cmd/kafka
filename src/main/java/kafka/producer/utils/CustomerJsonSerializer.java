package kafka.producer.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.producer.dto.Customer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class CustomerJsonSerializer implements Serializer<Customer> {

    @Override
    public byte[] serialize(String topic, Customer data) {
        try {
            return new ObjectMapper().writeValueAsString(data).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new SerializationException("Failed to serialize customer", e);
        }
    }
}

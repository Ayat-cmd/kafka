package kafka.consumer.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.consumer.dto.Customer;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class CustomerJsonDeserializer implements Deserializer<Customer> {
    @Override
    public Customer deserialize(String topic, byte[] data) {
        try {
            return new ObjectMapper().readValue(data, Customer.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}

package kafka.consumer.utils;

import kafka.consumer.dto.Customer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Objects;

public class CustomerDeserializer implements Deserializer<Customer> {
    @Override
    public Customer deserialize(String topic, byte[] data) {
        int id;
        int nameLength;
        String name;
        try {
            if (Objects.isNull(data)) {
                return null;
            }
            if (data.length < 8) {
                throw new SerializationException("Size of data received by CustomerDeserializer is less than 8 bytes");
            }

            ByteBuffer buffer = ByteBuffer.wrap(data);
            id = buffer.getInt();
            nameLength = buffer.getInt();
//            byte[] nameBytes = new byte[nameLength];
//            buffer.get(nameBytes);
//            name = new String(nameBytes, StandardCharsets.UTF_8);
            name = new String(data, buffer.position(), nameLength);
            return new Customer(name, id);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing data", e);
        }
    }
}

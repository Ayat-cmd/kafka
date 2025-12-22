package kafka.producer.utils;

import kafka.producer.dto.Customer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class CustomerSerializer implements Serializer<Customer> {

    @Override
    public byte[] serialize(String topic, Customer data) {
        try {
            byte[] serializeName;
            int stringSize;

            if (Objects.isNull(data)) {
                return null;
            } else {
                if (Objects.isNull(data.getName())) {
                    serializeName = new byte[0];
                    stringSize = 0;
                } else {
                    serializeName = data.getName().getBytes(StandardCharsets.UTF_8);
                    stringSize = data.getName().length();
                }
            }

            ByteBuffer buffer = ByteBuffer.allocate(4+4+stringSize);
            buffer.putInt(data.getId());
            buffer.putInt(stringSize);
            buffer.put(serializeName);

            return buffer.array();
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Customer to byte[] ", e);
        }
    }
}

package kafka.consumer.config;

import java.util.Properties;

public class KafkaConsumerConfig {
    private final Properties properties;

    public KafkaConsumerConfig() {
        this.properties = new Properties();
    }

    public KafkaConsumerConfig setProperties(String key, String value) {
        this.properties.put(key, value);
        return this;
    }

    public Properties build() {
        return this.properties;
    }
}

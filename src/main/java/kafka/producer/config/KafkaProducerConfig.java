package kafka.producer.config;

import java.util.Properties;

public class KafkaProducerConfig {
    private final Properties properties;

    public KafkaProducerConfig() {
        this.properties = new Properties();
    }

    public KafkaProducerConfig setProperty(String key, String value) {
        this.properties.put(key, value);
        return this;
    }

    public Properties build() {
        return this.properties;
    }
}

package kafka.consumer.config;

import java.util.Properties;

public class KafkaConsumerConfig {
    private final Properties properties;

    public KafkaConsumerConfig() {
        this.properties = new Properties();
    }

    public void setProperties(String key, String value) {
        this.properties.put(key, value);
    }

    public Properties build() {
        return this.properties;
    }
}

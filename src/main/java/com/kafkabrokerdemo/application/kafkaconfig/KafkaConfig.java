package com.kafkabrokerdemo.application.kafkaconfig;

import java.util.Properties;

public final class KafkaConfig {

    private KafkaConfig() {}

    public static final Properties properties = new Properties();

    static {
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

}

package com.example.kafkabrokerdemo.consumer;

import com.example.kafkabrokerdemo.event.Type1Event;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class StartConsumers {
    public static void main(String[] args) {

        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("group.id", "my-consumer-group");

        // exercise 4
        final int TYPE_1_EVENT_CONSUMERS_NB = 2;

        List<EventConsumer> type1EventConsumers = new ArrayList<>();
        List<Thread> type1EventThreads = new ArrayList<>();
        for (int i = 0; i < TYPE_1_EVENT_CONSUMERS_NB; i++) {
            EventConsumer p = new EventConsumer(prop, new Type1Event());
            type1EventConsumers.add(p);
            Thread t = new Thread(p);
            type1EventThreads.add(new Thread(p));
            t.start();
        }

    }
}

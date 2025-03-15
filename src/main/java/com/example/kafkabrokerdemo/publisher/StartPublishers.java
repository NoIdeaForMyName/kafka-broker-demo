package com.example.kafkabrokerdemo.publisher;

import com.example.kafkabrokerdemo.event.Type1Event;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class StartPublishers {
    public static void main(String[] args) {

        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // exercise 1
        final int TYPE_1_EVENT_PUBLISHERS_NB = 3;
        final int TYPE_1_EVENT_PUBLISHER_INTERVAL_MS = 1000;

        List<EventPublisher> type1EventPublishers = new ArrayList<>();
        List<Thread> type1EventThreads = new ArrayList<>();
        for (int i = 0; i < TYPE_1_EVENT_PUBLISHERS_NB; i++) {
            EventPublisher p = new EventPublisher(prop, new Type1Event(), TYPE_1_EVENT_PUBLISHER_INTERVAL_MS, false);
            type1EventPublishers.add(p);
            Thread t = new Thread(p);
            type1EventThreads.add(new Thread(p));
            t.start();
        }


    }
}
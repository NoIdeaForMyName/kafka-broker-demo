package com.example.kafkabrokerdemo.consumer;

import com.example.kafkabrokerdemo.event.Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EventConsumer implements Runnable {

    private final int POLL_DURATION_MS = 100;

    private final KafkaConsumer<String, String> consumer;
    private final Event event;
    private boolean start;

    public EventConsumer(Properties kafkaProp, Event event) {
        consumer = new KafkaConsumer<>(kafkaProp);
        this.event = event;
        consumer.subscribe(Collections.singleton(event.getTopic()));
        this.start = false;
    }

    @Override
    public void run() {
        start = true;
        while(start) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_DURATION_MS));
            for(ConsumerRecord<String, String> record: records) {
                System.out.println("Received message: " + record.value());
            }
            consumer.commitSync();
        }
        consumer.close();
    }

    public void stop() {
        start = false;
    }

}

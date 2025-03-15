package com.example.kafkabrokerdemo.consumer;

import com.example.kafkabrokerdemo.event.Event;
import com.example.kafkabrokerdemo.kafkaconfig.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EventConsumer implements Runnable {

    protected static int consumerNb = 0;
    protected final int consumerID;

    protected final int POLL_DURATION_MS = 100;

    protected final KafkaConsumer<String, String> consumer;
    protected final Event event;
    protected boolean start;

    public EventConsumer(Event event) {
        consumerNb++;
        consumerID = consumerNb;

        Properties prop = KafkaConfig.properties;
        prop.put("group.id", "consumer-group-" + consumerID);

        consumer = new KafkaConsumer<>(KafkaConfig.properties);
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
                System.out.println(toString() + "; Received message: " + record.value());
            }
            consumer.commitSync();
        }
        consumer.close();
    }

    public void stop() {
        start = false;
    }

    @Override
    public String toString() {
        return String.format("Consumer:[ID-%d]; timestamp: %s", consumerID, new Timestamp(System.currentTimeMillis()));
    }
}

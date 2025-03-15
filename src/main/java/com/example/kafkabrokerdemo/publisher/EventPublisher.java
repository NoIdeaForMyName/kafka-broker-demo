package com.example.kafkabrokerdemo.publisher;

/*
- typ zdarzenia (typ generyczny)
- properties
- max. przedzial czasowy
- czy losowo
- akcja po opublikowaniu
 */

import com.example.kafkabrokerdemo.kafkaconfig.KafkaConfig;
import org.apache.kafka.clients.producer.*;
import com.example.kafkabrokerdemo.event.Event;

import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class EventPublisher implements Runnable {

    private static int publishersNb = 0;
    private final int publisherID;

    private final int maxTimeIntervalMs;
    private final boolean randomInterval;
    private final KafkaProducer<String, String> producer;
    private final Event event;
    private boolean start;

    public EventPublisher(Event event, int maxTimeIntervalMs, boolean randomInterval) {
        this.maxTimeIntervalMs = maxTimeIntervalMs;
        this.randomInterval = randomInterval;
        this.producer = new KafkaProducer<>(KafkaConfig.properties);
        this.event = event;
        this.start = false;

        publishersNb++;
        publisherID = publishersNb;
    }

    @Override
    public void run() {
        start = true;
        while(start) {
            ProducerRecord<String, String> record = new ProducerRecord<>(event.getTopic(), null, toString());
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("Record sent successfully");
                    }
                    else {
                        System.out.println("Some exception occurred: " + e.getMessage());
                    }
                }
            }); // TODO callback
            try {
                TimeUnit.MILLISECONDS.sleep(randomInterval ? (int) (Math.random() * maxTimeIntervalMs) : maxTimeIntervalMs);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        producer.close();
    }

    public void stop() {
        start = false;
    }

    @Override
    public String toString() {
        return String.format("Publisher:[ID-%d; Topic-%s; MaxTimeInterval-%d; randomInterval-%b]; timestamp: %s", publisherID, event.getTopic(), maxTimeIntervalMs, randomInterval, new Timestamp(System.currentTimeMillis()));
    }
}

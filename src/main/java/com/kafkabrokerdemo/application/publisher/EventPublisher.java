package com.kafkabrokerdemo.application.publisher;

/*
- typ zdarzenia (typ generyczny)
- properties
- max. przedzial czasowy
- czy losowo
- akcja po opublikowaniu
 */

import com.kafkabrokerdemo.application.kafkaconfig.KafkaConfig;
import org.apache.kafka.clients.producer.*;
import com.kafkabrokerdemo.domain.event.Event;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class EventPublisher implements Runnable {

    private static int publishersNb = 0;
    private final int publisherID;

    private final int maxTimeIntervalMs;
    private final boolean randomInterval;
    private final KafkaProducer<String, String> producer;
    private final Event event;
    private boolean start;

    private static final Logger logger = LogManager.getLogger(EventPublisher.class);

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
        logger.info("{} started with {} running", Thread.currentThread().getName(), toString());
        start = true;
        while(start) {
            ProducerRecord<String, String> record = new ProducerRecord<>(event.getTopic(), null, toString());
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Record sent successfully:[{}]", event.getTopic());
                    }
                    else {
                        logger.error("Some exception occurred: {}", e.getMessage());
                    }
                }
            });
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
        logger.info("{} stopped with {} inside", Thread.currentThread().getName(), toString());
    }

    @Override
    public String toString() {
        return String.format("Publisher:[ID-%d; Topic-%s; MaxTimeInterval-%d; randomInterval-%b]", publisherID, event.getTopic(), maxTimeIntervalMs, randomInterval);
    }
}

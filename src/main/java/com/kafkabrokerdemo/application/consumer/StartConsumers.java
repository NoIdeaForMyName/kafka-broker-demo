package com.kafkabrokerdemo.application.consumer;

import com.kafkabrokerdemo.domain.event.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StartConsumers {

    private static final Logger logger = LogManager.getLogger(StartConsumers.class);

    public static void main(String[] args) {

        // exercise 4
        final int TYPE_1_EVENT_CONSUMERS_NB = 2;
        final Event TYPE_1_EVENT = new Type1Event();
        startConsumers(TYPE_1_EVENT_CONSUMERS_NB, TYPE_1_EVENT);

        // exercise 5
        final int TYPE_2_EVENT_CONSUMERS_NB = 1;
        final Event TYPE_2_EVENT = new Type2Event();
        startConsumers(TYPE_2_EVENT_CONSUMERS_NB, TYPE_2_EVENT);

        // exercise 6
        final int TYPE_3_EVENT_CONSUMERS_NB = 1;
        logger.info("Starting {} customers; event type: {}", TYPE_3_EVENT_CONSUMERS_NB, new Type3Event().getTopic());
        for (int i = 0; i < TYPE_3_EVENT_CONSUMERS_NB; i++) {
            EventConsumer c = new Type3EventConsumer();
            Thread t = new Thread(c);
            t.start();
        }

        // exercise 7
        final int TYPE_4_EVENT_CONSUMERS_NB = 1;
        final Event TYPE_4_EVENT = new Type4Event();
        startConsumers(TYPE_4_EVENT_CONSUMERS_NB, TYPE_4_EVENT);

    }

    private static void startConsumers(int n, Event event) {
        logger.info("Starting {} customers; event type: {}", n, event.getTopic());
        for (int i = 0; i < n; i++) {
            EventConsumer c = new EventConsumer(event);
            Thread t = new Thread(c);
            t.start();
        }
    }

}

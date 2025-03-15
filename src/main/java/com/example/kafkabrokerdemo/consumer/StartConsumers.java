package com.example.kafkabrokerdemo.consumer;

import com.example.kafkabrokerdemo.event.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class StartConsumers {
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
        //final Event TYPE_3_EVENT = new Type3Event();
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
        for (int i = 0; i < n; i++) {
            EventConsumer c = new EventConsumer(event);
            Thread t = new Thread(c);
            t.start();
        }
    }

}

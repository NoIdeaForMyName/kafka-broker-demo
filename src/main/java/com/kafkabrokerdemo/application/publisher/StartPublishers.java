package com.kafkabrokerdemo.application.publisher;

import com.kafkabrokerdemo.domain.event.Event;
import com.kafkabrokerdemo.domain.event.Type1Event;
import com.kafkabrokerdemo.domain.event.Type2Event;
import com.kafkabrokerdemo.domain.event.Type3Event;

public class StartPublishers {
    public static void main(String[] args) {

        // exercise 1
        final int TYPE_1_EVENT_PUBLISHERS_NB = 3;
        final int TYPE_1_EVENT_PUBLISHER_INTERVAL_MS = 1000;
        final Event TYPE_1_EVENT = new Type1Event();
        final boolean TYPE_1_RANDOM_INTERVAL = false;
        startPublishers(TYPE_1_EVENT_PUBLISHERS_NB, TYPE_1_EVENT_PUBLISHER_INTERVAL_MS, TYPE_1_RANDOM_INTERVAL, TYPE_1_EVENT);

        // exercise 2
        final int TYPE_2_EVENT_PUBLISHERS_NB = 1;
        final int TYPE_2_EVENT_PUBLISHER_INTERVAL_MS = 2000;
        final Event TYPE_2_EVENT = new Type2Event();
        final boolean TYPE_2_RANDOM_INTERVAL = true;
        startPublishers(TYPE_2_EVENT_PUBLISHERS_NB, TYPE_2_EVENT_PUBLISHER_INTERVAL_MS, TYPE_2_RANDOM_INTERVAL, TYPE_2_EVENT);

        // exercise 3
        final int TYPE_3_EVENT_PUBLISHERS_NB = 1;
        final int TYPE_3_EVENT_PUBLISHER_INTERVAL_MS = 5000;
        final Event TYPE_3_EVENT = new Type3Event();
        final boolean TYPE_3_RANDOM_INTERVAL = true;
        startPublishers(TYPE_3_EVENT_PUBLISHERS_NB, TYPE_3_EVENT_PUBLISHER_INTERVAL_MS, TYPE_3_RANDOM_INTERVAL, TYPE_3_EVENT);

    }

    private static void startPublishers(int n, int publishInterval, boolean randomInterval, Event event) {
        //List<EventPublisher> publishers = new ArrayList<>();
        //List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            EventPublisher p = new EventPublisher(event, publishInterval, randomInterval);
            //publishers.add(p);
            Thread t = new Thread(p);
            //threads.add(new Thread(p));
            t.start();
        }
    }

}
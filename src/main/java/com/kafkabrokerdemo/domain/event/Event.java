package com.kafkabrokerdemo.domain.event;

public abstract class Event {

    public String getTopic() {
        return this.getClass().getName();
    }

}

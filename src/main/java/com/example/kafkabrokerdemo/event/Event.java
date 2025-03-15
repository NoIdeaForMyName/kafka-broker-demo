package com.example.kafkabrokerdemo.event;

public abstract class Event {

    public String getTopic() {
        return this.getClass().getName();
    }

}

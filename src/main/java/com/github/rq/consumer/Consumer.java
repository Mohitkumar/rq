package com.github.rq.consumer;

public interface Consumer<T> {
    void start();

    void init();

    void stop() throws InterruptedException;
}

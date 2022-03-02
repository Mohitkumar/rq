package com.github.rq.consumer;

public interface Consumer<T> {
    void start();

    void stop() throws InterruptedException;
}

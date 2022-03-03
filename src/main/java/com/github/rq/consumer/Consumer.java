package com.github.rq.consumer;

/**
 * Consumer consume the message from the Queue.
 * @param <T>
 */
public interface Consumer<T> {
    void start();

    void stop() throws InterruptedException;
}

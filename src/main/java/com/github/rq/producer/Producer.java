package com.github.rq.producer;

/**
 * Producer add the messages to queue.
 * @param <T>
 */
public interface Producer<T> {

    void submit(T t);
}

package com.github.rq;

/**
 * This is the interface which should be implemented by the client of consumers.
 * Implementation of this interface should have the processing logic for the message.
 * Once a message is consumed by the consumer this listener is called
 * @param <T>
 */
public interface ConsumerListener<T> {
    void onMessage(Message<T> t, String consumerName) throws RetryableException;
}

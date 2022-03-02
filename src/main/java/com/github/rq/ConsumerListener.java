package com.github.rq;

public interface ConsumerListener<T> {
    void onMessage(Message<T> t, String consumerName) throws RetryableException;
}

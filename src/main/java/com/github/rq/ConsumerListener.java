package com.github.rq;

public interface ConsumerListener<T> {
    void onMessage(Message<T> t) throws RetryableException;
}

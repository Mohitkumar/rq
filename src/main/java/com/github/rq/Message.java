package com.github.rq;

public class Message<T> {

    public Message() {
    }

    private T payload;

    private int retryCount;

    public Message(T payload) {
        this.payload = payload;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public T getPayload() {
        return payload;
    }
}

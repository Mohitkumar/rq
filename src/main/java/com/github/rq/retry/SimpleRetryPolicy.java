package com.github.rq.retry;

public class SimpleRetryPolicy implements RetryPolicy{
    private int totalRetries;

    public SimpleRetryPolicy(int totalRetries) {
        this.totalRetries = totalRetries;
    }

    @Override
    public boolean shouldRetry(int currentRetry, long currentRetryTime) {
        return currentRetry < totalRetries;
    }
}

package com.github.rq.retry;

public interface RetryPolicy {

    boolean shouldRetry(int currentRetry, long currentRetryTime);
}

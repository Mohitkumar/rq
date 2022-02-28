package com.github.rq;

public class RetryableException extends Exception{

    public RetryableException(String message) {
        super(message);
    }
}

package com.github.rq;

public class SerializationException extends RuntimeException{
    public SerializationException(String s) {
        super(s);
    }

    public SerializationException(String s, Throwable throwable) {
        super(s, throwable);
    }
}

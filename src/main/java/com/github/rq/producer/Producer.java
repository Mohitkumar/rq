package com.github.rq.producer;

public interface Producer<T> {

    void submit(T t);
}

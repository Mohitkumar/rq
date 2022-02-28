package com.github.rq.producer;

import com.github.rq.Message;
import com.github.rq.queue.Queue;

public class DefaultProducer<T> implements Producer<T> {

    private Queue<T> queue;

    public DefaultProducer(Queue<T> queue) {
        this.queue = queue;
    }

    @Override
    public void submit(T t) {
        Message<T> m = new Message<>(t);
        this.queue.enqueue(m);
    }
}

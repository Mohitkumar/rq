package com.github.rq.queue;

import com.github.rq.Message;
import com.github.rq.serializer.MessageSerializer;

public interface Queue<T> {

    String getName();

    void enqueue(Message<T> message);

    Message<T> dequeue();

    MessageSerializer getMessageSerializer();

    void transferTo(Queue<T> queue);
}

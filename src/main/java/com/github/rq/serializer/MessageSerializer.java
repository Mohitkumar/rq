package com.github.rq.serializer;

import com.github.rq.Message;

public interface MessageSerializer<T> {
    String serialize(Message<T> m);

    Message<T> deserialize(String message, Class<T> clazz);
}

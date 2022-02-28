package com.github.rq.serializer;

public interface MessageSerializer {
    String serialize(Object o);

    <T> T deserialize(String message, Class<T> type);
}

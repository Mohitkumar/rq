package com.github.rq.serializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rq.Message;
import com.github.rq.SerializationException;

import java.io.IOException;

public class JacksonMessageSerializer<T> implements MessageSerializer<T>{
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public String serialize(Message<T> m) {
        try {
            return mapper.writeValueAsString(m);
        } catch (IOException e) {
            throw new SerializationException("Could not serialize object using Jackson.", e);
        }
    }

    @Override
    public Message<T> deserialize(String payload) {
        if (payload == null) {
            return null;
        }
        try {
            return mapper.readValue(payload, new TypeReference<Message<T>>() {});
        } catch (IOException e) {
            throw new SerializationException("Could not serialize object using Jackson.", e);
        }
    }
}

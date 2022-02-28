package com.github.rq.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rq.SerializationException;

import java.io.IOException;

public class JacksonMessageSerializer implements MessageSerializer{
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public String serialize(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (IOException e) {
            throw new SerializationException("Could not serialize object using Jackson.", e);
        }
    }

    @Override
    public <T> T deserialize(String payload, Class<T> type) {
        if (payload == null) {
            return null;
        }

        try {
            return mapper.readValue(payload, type);
        } catch (IOException e) {
            throw new SerializationException("Could not serialize object using Jackson.", e);
        }
    }
}

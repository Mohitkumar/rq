package com.github.rq.serializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rq.Message;
import com.github.rq.SerializationException;

import javax.xml.crypto.Data;
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

    public Message<T> deserialize(String payload, Class<T> clazz) {
        if (payload == null) {
            return null;
        }
        try {
            JavaType javaType = mapper.getTypeFactory()
                    .constructParametricType(Message.class, clazz);
            return mapper.readValue(payload, javaType);
        } catch (IOException e) {
            throw new SerializationException("Could not deserialize object using Jackson.", e);
        }
    }
}

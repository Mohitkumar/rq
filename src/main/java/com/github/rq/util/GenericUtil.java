package com.github.rq.util;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class GenericUtil {

    public static Class<?> getGenericTypeOfInterface(Class<?> clazz, Class<?> specificInterface) {
        Type[] genericInterfaces = clazz.getGenericInterfaces();
        if (genericInterfaces != null) {
            for (Type genericType : genericInterfaces) {
                if (genericType instanceof ParameterizedType) {
                    Type rawType = ((ParameterizedType) genericType).getRawType();
                    if (rawType.equals(specificInterface)) {
                        ParameterizedType paramType = (ParameterizedType) genericType;
                        return (Class<?>) paramType.getActualTypeArguments()[0];
                    }
                }
            }
        }
        return null;
    }
}

package com.github.rq.redis;

import java.util.List;
import java.util.Set;

public interface IRedisClient {
    void leftPush(String queue, String... entry);

    void blockingRightPopAndLeftPush(String from, String to, int timeout);

    String blockingRightPop(String queue, int timeout);

    void addToSet(String key, String... members);

    void removeFromSet(String key, String... members);

    Set<String> sMembers(String key);

    List<String> lRange(String key, long start, long end);

    void delete(String... keys);

}

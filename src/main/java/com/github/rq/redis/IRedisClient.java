package com.github.rq.redis;

import java.util.List;
import java.util.Set;

/**
 * It is a wrapper interface over the redis operations.
 */
public interface IRedisClient {
    void leftPush(String queue, String... entry);

    void brpoplpush(String from, String to, int timeout);

    String brpop(String queue, int timeout);

    void sadd(String key, String... members);

    void srem(String key, String... members);

    Set<String> sMembers(String key);

    List<String> lRange(String key, long start, long end);

    void delete(String... keys);

    String bzpopmax(String queue, int timeout);

    void zadd(String queue, String entry, long score);

    List<String> zRange(String key, long start, long end);
}

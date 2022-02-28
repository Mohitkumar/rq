package com.github.rq.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class RedisClient implements IRedisClient{

    private static final Logger logger = LoggerFactory.getLogger(RedisClient.class);

    protected JedisManager jedisManager;

    public void configure(String host, int port) throws Exception {
        this.jedisManager = new JedisManager(host, port);
        logger.info("redis client: {} initialised ...", port);
    }

    public void configure(String host, int port, String password) throws Exception {
        this.jedisManager = new JedisManager(host, port, password);
        logger.info("redis client: {} initialised...", port);
    }

    @Override
    public void leftPush(String queue, String... entry) {
        Jedis j = null;
        try {
            j = jedisManager.getConnection();
            j.lpush(queue, entry);
        } catch(Exception e) {
            logger.error("could not left push ot queue: " + queue, e);
        } finally {
            if (j != null) {
                jedisManager.returnConnection(j);
            }
        }
    }

    public void blockingRightPopAndLeftPush(String from, String to, int timeout) {
        Jedis j = null;
        try {
            j = jedisManager.getConnection();
            j.brpoplpush(from, to, timeout);
        } catch(Exception e) {
            logger.error("could not do rightPopAndLeftPush", e);
        } finally {
            if (j != null) {
                jedisManager.returnConnection(j);
            }
        }
    }

    public String blockingRightPop(String queue, int timeout) {
        Jedis j = null;
        try {
            j = jedisManager.getConnection();
            List<String> elements = j.brpop(timeout, queue);
            return elements.isEmpty() ? null : elements.get(1);
        } catch(Exception e) {
            logger.error("could not do rightPopAndLeftPush", e);
        } finally {
            if (j != null) {
                jedisManager.returnConnection(j);
            }
        }
        return null;
    }

    public void addToSet(String key, String... members) {
        Jedis j = null;
        try {
            j = jedisManager.getConnection();
            j.sadd(key, members);
        } catch(Exception e) {
            logger.error("could not do rightPopAndLeftPush", e);
        } finally {
            if (j != null) {
                jedisManager.returnConnection(j);
            }
        }
    }

    public void removeFromSet(String key, String... members) {
        Jedis j = null;
        try {
            j = jedisManager.getConnection();
            j.srem(key, members);
        } catch(Exception e) {
            logger.error("could not do rightPopAndLeftPush", e);
        } finally {
            if (j != null) {
                jedisManager.returnConnection(j);
            }
        }
    }

    public Set<String> sMembers(String key) {
        Jedis j = null;
        try {
            j = jedisManager.getConnection();
            return j.smembers(key);
        } catch(Exception e) {
            logger.error("could not do rightPopAndLeftPush", e);
        } finally {
            if (j != null) {
                jedisManager.returnConnection(j);
            }
        }
        return Collections.emptySet();
    }

    public List<String> lRange(String key, long start, long end) {
        Jedis j = null;
        try {
            j = jedisManager.getConnection();
            return j.lrange(key, start, end);
        } catch(Exception e) {
            logger.error("could not do rightPopAndLeftPush", e);
        } finally {
            if (j != null) {
                jedisManager.returnConnection(j);
            }
        }
        return Collections.emptyList();
    }

    public void delete(String... keys) {
        Jedis j = null;
        try {
            j = jedisManager.getConnection();
            j.del(keys);
        } catch(Exception e) {
            logger.error("could not do rightPopAndLeftPush", e);
        } finally {
            if (j != null) {
                jedisManager.returnConnection(j);
            }
        }
    }
}

package com.github.rq.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.resps.KeyedZSetElement;
import redis.clients.jedis.resps.Tuple;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class RedisClient implements IRedisClient{

    private static final Logger logger = LoggerFactory.getLogger(RedisClient.class);

    protected JedisManager jedisManager;

    public RedisClient(String host, int port) {
        this.jedisManager = new JedisManager(host, port);
    }

    public RedisClient(String host, int port, String password) {
        this.jedisManager = new JedisManager(host, port, password);
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

    @Override
    public void zadd(String queue, String entry, long score) {
        Jedis j = null;
        try {
            j = jedisManager.getConnection();
            j.zadd(queue, score, entry);
        } catch(Exception e) {
            logger.error("could not left push to queue: " + queue, e);
        } finally {
            if (j != null) {
                jedisManager.returnConnection(j);
            }
        }
    }
    @Override
    public String bzpopmax(String queue, int timeout) {
        Jedis j = null;
        try {
            j = jedisManager.getConnection();
            Tuple tuple = j.bzpopmax(timeout,queue);
            return tuple.getElement();
        } catch(Exception e) {
            logger.error("could not left push ot queue: " + queue, e);
        } finally {
            if (j != null) {
                jedisManager.returnConnection(j);
            }
        }
        return null;
    }

    public void brpoplpush(String from, String to, int timeout) {
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

    public void bzpopMaxlpush(String from, String to, int timeout) {
        Jedis j = null;
        try {
            j = jedisManager.getConnection();
            KeyedZSetElement zSetElement = j.bzpopmax(timeout, from);
            j.lpush(to, zSetElement.getElement());
        } catch(Exception e) {
            logger.error("could not do rightPopAndLeftPush", e);
        } finally {
            if (j != null) {
                jedisManager.returnConnection(j);
            }
        }
    }

    public String brpop(String queue, int timeout) {
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

    public void sadd(String key, String... members) {
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

    public void srem(String key, String... members) {
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

    public List<String> zRange(String key, long start, long end) {
        Jedis j = null;
        try {
            j = jedisManager.getConnection();
            return j.zrangeByScore(key, start, end);
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

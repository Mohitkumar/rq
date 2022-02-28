package com.github.rq.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RedisClusterClient implements IRedisClient{

    private static final Logger logger = LoggerFactory.getLogger(RedisClusterClient.class);

    private JedisCluster jedisCluster;

    private int minIdleConnections = 5;
    private int maxIdleConnections = 100;
    private int maxConnections = 500;
    private int maxWait = 10000;

    public void configure(String[] hostUrls, String password) throws Exception {
        Set<HostAndPort> clusterNodes = new HashSet<>(hostUrls.length);
        for (String each : hostUrls) {
            String[] x = each.split(":");
            clusterNodes.add(new HostAndPort(x[0], Integer.parseInt(x[1])));
        }
        GenericObjectPoolConfig poolConfig =new GenericObjectPoolConfig();
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);
        poolConfig.setTestWhileIdle(false);
        poolConfig.setMinIdle(minIdleConnections);
        poolConfig.setMaxIdle(maxIdleConnections);
        poolConfig.setMaxTotal(maxConnections);
        poolConfig.setMaxWaitMillis(maxWait);
        poolConfig.setBlockWhenExhausted(true);
        jedisCluster = new JedisCluster(clusterNodes, 2000, 1000*10, 5, password, poolConfig);
        logger.info("redis cluster client initialised...");
    }

    @Override
    public void leftPush(String queue, String... entry) {
        jedisCluster.lpush(queue, entry);
    }

    @Override
    public void blockingRightPopAndLeftPush(String from, String to, int timeout) {
        jedisCluster.brpoplpush(from, to, timeout);
    }

    @Override
    public String blockingRightPop(String queue, int timeout) {
        List<String> brpop = jedisCluster.brpop(timeout, queue);
        return brpop.isEmpty() ? null : brpop.get(1);
    }

    @Override
    public void addToSet(String key, String... members) {
        jedisCluster.sadd(key, members);
    }

    @Override
    public void removeFromSet(String key, String... members) {
        jedisCluster.srem(key, members);
    }

    @Override
    public Set<String> sMembers(String key) {
        return jedisCluster.smembers(key);
    }

    @Override
    public List<String> lRange(String key, long start, long end) {
        return jedisCluster.lrange(key, start, end);
    }

    @Override
    public void delete(String... keys) {
        jedisCluster.del(keys);
    }
}

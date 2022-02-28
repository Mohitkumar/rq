package com.github.rq.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

import java.io.IOException;

public class JedisManager {

	private static final Logger logger = LoggerFactory.getLogger(JedisManager.class);

	private JedisPool jedisPool;
	private String host = null;
	private int port = 6379;
	private String password = null;

	private int maxRetriesToGetJedisConn = 5;
	private int sleepTimeMsWhileGettingJedisConn = 2000;

	private int minIdleConnections = 5;
	private int maxIdleConnections = 100;
	private int maxConnections = 500;
	private int maxWait = 10000;

	public JedisManager(String host, int port) {
		this.host = host;
		this.port = port;
		createPool();
	}
	
	public JedisManager(String host, int port, String password) {
		this.host = host;
		this.port = port;
		this.password = password;
		createPool();
	}

	public JedisManager(String host, int port, int maxConnections) {
		this.host = host;
		this.port = port;
		this.maxConnections = maxConnections;
		createPool();
	}

	public JedisManager(String host, int port, int maxRetries, int minIdleConnections, int maxIdleConnections,
                        int maxConnections, int maxWaitTime) {
		this.host = host;
		this.port = port;
		this.maxRetriesToGetJedisConn = maxRetries;
		this.minIdleConnections = minIdleConnections;
		this.maxIdleConnections = maxIdleConnections;
		this.maxConnections = maxConnections;
		this.maxWait = maxWaitTime;
		createPool();
	}

	private void createPool() {
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setTestOnBorrow(false);
		poolConfig.setTestOnReturn(false);
		poolConfig.setTestWhileIdle(false);
		poolConfig.setMinIdle(this.minIdleConnections);
		poolConfig.setMaxIdle(this.maxIdleConnections);
		poolConfig.setMaxTotal(this.maxConnections);
		poolConfig.setMaxWaitMillis(this.maxWait);
		poolConfig.setBlockWhenExhausted(true);
		if(this.password ==null)
			jedisPool = new JedisPool(poolConfig, this.host, this.port, this.maxWait);
		else
			jedisPool = new JedisPool(poolConfig, this.host, this.port, this.maxWait, this.password);
	}

	private void destroyPool() {
		jedisPool.destroy();
	}

	private void recreatePool() {
		destroyPool();
		createPool();
	}

	public Jedis getConnection() throws IOException, InterruptedException {
		Jedis j = null;
		for (int i = 0; i < this.maxRetriesToGetJedisConn; i++) {
			try {
				j = jedisPool.getResource();
				if (j != null) {
					return j;
				}
			} catch (Exception e) {
				logger.warn("jedis connection not available, retry no.: {}, reason: {}", i + 1, e.getMessage());
				logger.info("sleeping for {} ms", sleepTimeMsWhileGettingJedisConn);
				Thread.sleep(sleepTimeMsWhileGettingJedisConn);
			}
		}
		recreatePool();
		throw new IOException("could not get redis connection");
	}

	public void returnConnection(Jedis jedis) {
		try {
			if (jedis != null)
				jedis.close();
		} catch (JedisException e) {
			logger.error("could not return redis connection");
		}
	}
}

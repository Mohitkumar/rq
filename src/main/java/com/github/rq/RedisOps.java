package com.github.rq;

import com.github.rq.redis.IRedisClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RedisOps {
    private final String nameSpace;

    private final IRedisClient redisClient;

    private static final int TIMEOUT = 0;

    public RedisOps(String nameSpace, IRedisClient redisClient) {
        this.nameSpace = nameSpace;
        this.redisClient = redisClient;
    }

    public void leftPush(String queue, String message){
        redisClient.leftPush(String.format("%s.%s",nameSpace,queue), message);
    }

    public void zadd(String queue, String message, long priority){
        redisClient.zadd(String.format("%s.%s",nameSpace,queue), message, priority);
    }

    public void transferMessage(String fromQueue, String toQueue){
        redisClient.brpoplpush(String.format("%s.%s",nameSpace,fromQueue),
                String.format("%s.%s",nameSpace,toQueue), TIMEOUT);
    }

    public String popMessage(String queue){
        return redisClient.brpop(String.format("%s.%s",nameSpace,queue),TIMEOUT);
    }

    public String bzpopmax(String queue){
        return redisClient.bzpopmax(String.format("%s.%s",nameSpace,queue),TIMEOUT);
    }


    public String registerConsumer(String queueName,String consumer){
        String consumeName = String.format("%s.%s.consumer.%s", nameSpace, queueName, consumer);
        redisClient.sadd(String.format("%s.%s.consumers",nameSpace, queueName),
                consumeName);
        return consumeName;
    }

    public void removeConsumer(String queueName, String consumer){
        redisClient.srem(String.format("%s.%s.consumers",nameSpace, queueName), consumer);
    }

    public List<String> getConsumers(String queueName){
        Set<String> members = redisClient.sMembers(String.format("%s.%s.consumers", nameSpace, queueName));
        if(members != null){
            return  new ArrayList<>(members);
        }
        return new ArrayList<>();
    }

    public void copyList(String to, String from){
        List<String> values = redisClient.lRange(from, 0, -1);
        String[] valuesArr = values.toArray(new String[values.size()]);
        if(values != null && !values.isEmpty()){
            redisClient.leftPush(String.format("%s.%s",nameSpace,to), valuesArr);
            redisClient.delete(from);
        }
    }
}

package com.github.rq;

import com.github.rq.redis.IRedisClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RedisOps {
    private String nameSpace;

    private IRedisClient redisClient;

    private static int TIMEOUT = 0;

    public RedisOps(String nameSpace, IRedisClient redisClient) {
        this.nameSpace = nameSpace;
        this.redisClient = redisClient;
    }

    public void addMessageToQueue(String queue, String message){
        redisClient.leftPush(String.format("%s.%s",nameSpace,queue), message);
    }

    public void transferMessage(String fromQueue, String toQueue){
        redisClient.blockingRightPopAndLeftPush(String.format("%s.%s",nameSpace,fromQueue),
                String.format("%s.%s",nameSpace,toQueue), TIMEOUT);
    }

    public String popMessage(String queue){
        return redisClient.blockingRightPop(String.format("%s.%s",nameSpace,queue),TIMEOUT);
    }

    public String registerConsumer(String queueName,String consumer){
        String consumeName = String.format("%s.%s.consumer.%s", nameSpace, queueName, consumer);
        redisClient.addToSet(String.format("%s.%s.consumers",nameSpace, queueName),
                consumeName);
        return consumeName;
    }

    public void removeConsumer(String queueName, String consumer){
        redisClient.removeFromSet(String.format("%s.%s.consumers",nameSpace, queueName), consumer);
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

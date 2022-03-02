package com.github.rq.queue;


import com.github.rq.Message;
import com.github.rq.RedisOps;
import com.github.rq.serializer.MessageSerializer;

public class RedisQueue<T> implements Queue<T>{

    private RedisOps redisOps;

    private MessageSerializer messageSerializer;

    private String queueName;

    public RedisQueue(RedisOps redisOps, MessageSerializer messageSerializer, String queueName) {
        this.redisOps = redisOps;
        this.messageSerializer = messageSerializer;
        this.queueName = queueName;
    }

    public MessageSerializer getMessageSerializer() {
        return messageSerializer;
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public void enqueue(Message<T> message) {
        String data = messageSerializer.serialize(message);
        redisOps.leftPush(queueName, data);
    }

    @Override
    public Message<T> dequeue() {
        String data = redisOps.popMessage(queueName);
        if(data != null){
            return messageSerializer.deserialize(data);
        }
        return null;
    }

    public void transferTo(Queue<T> queue){
        redisOps.transferMessage(queueName, queue.getName());
    }
}

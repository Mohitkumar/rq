package com.github.rq.queue;


import com.github.rq.Message;
import com.github.rq.RedisOps;
import com.github.rq.serializer.MessageSerializer;
import com.github.rq.util.GenericUtil;

public class RedisQueue<T> implements Queue<T>{

    private RedisOps redisOps;

    private MessageSerializer messageSerializer;

    private String queueName;

    private Class<T> type;

    public RedisQueue(RedisOps redisOps, MessageSerializer messageSerializer, String queueName) {
        this.redisOps = redisOps;
        this.messageSerializer = messageSerializer;
        this.queueName = queueName;
    }

    public void inferType(Class<?> clazz, Class<?> specificInterface) {
        this.type = (Class<T>) GenericUtil.getGenericTypeOfInterface(clazz, specificInterface);
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
        String data = messageSerializer.serialize(message.getPayload());
        redisOps.addMessageToQueue(queueName, data);
    }

    @Override
    public Message<T> dequeue() {
        String data = redisOps.popMessage(queueName);
        if(data != null){
            T message = messageSerializer.deserialize(data, type);
            return new Message<T>(message);
        }
        return null;
    }

    public void transferTo(Queue<T> queue){
        redisOps.transferMessage(queueName, queue.getName());
    }


}

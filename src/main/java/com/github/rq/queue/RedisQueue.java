package com.github.rq.queue;


import com.github.rq.Message;
import com.github.rq.serializer.MessageSerializer;
import com.github.rq.util.GenericUtil;

/**
 * <pre>
 * Queue implementation backed by the redis List. It uses blocking redis operations to pop the message from queue.
 * enqueue uses the rpush redis operation.
 * dequeue uses the blpop to dequeue the message from queue and blocks until new message is available.
 * transferTo uses the blpoprpush to atomically transfer the message from this queue to the queue provided in paramter
 * </pre>
 * @param <T>
 */
public class RedisQueue<T> implements Queue<T>{

    protected RedisOps redisOps;

    protected MessageSerializer<T> messageSerializer;

    protected String queueName;

    protected Class<T> type;

    public RedisQueue(RedisOps queueOps, MessageSerializer<T> messageSerializer, String queueName) {
        this.redisOps = queueOps;
        this.messageSerializer = messageSerializer;
        this.queueName = queueName;
    }

    public MessageSerializer<T> getMessageSerializer() {
        return messageSerializer;
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public RedisOps getRedisOps() {
        return redisOps;
    }

    @Override
    public void enqueue(Message<T> message) {
        String data = messageSerializer.serialize(message);
        redisOps.pushMessage(queueName, data);
    }

    @Override
    public Message<T> dequeue() {
        String data = redisOps.popMessage(queueName);
        if(data != null){
            return messageSerializer.deserialize(data, this.type);
        }
        return null;
    }

    public void transferTo(Queue<T> queue){
        redisOps.transferMessage(queueName, queue.getName());
    }

    public void inferType(Class<?> clazz, Class<?> specificInterface) {
        this.type = (Class<T>) GenericUtil.getGenericTypeOfInterface(clazz, specificInterface);
    }
}

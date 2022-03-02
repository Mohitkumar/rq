package com.github.rq.queue;


import com.github.rq.Message;
import com.github.rq.serializer.MessageSerializer;

public class RedisQueue<T> implements Queue<T>{

    private QueueOps queueOps;

    private MessageSerializer<T> messageSerializer;

    private String queueName;

    public RedisQueue(QueueOps queueOps, MessageSerializer<T> messageSerializer, String queueName) {
        this.queueOps = queueOps;
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
    public QueueOps getQueueOps() {
        return queueOps;
    }

    @Override
    public void enqueue(Message<T> message) {
        String data = messageSerializer.serialize(message);
        queueOps.leftPush(queueName, data);
    }

    @Override
    public Message<T> dequeue() {
        String data = queueOps.popMessage(queueName);
        if(data != null){
            return messageSerializer.deserialize(data);
        }
        return null;
    }

    public void transferTo(Queue<T> queue){
        queueOps.transferMessage(queueName, queue.getName());
    }
}

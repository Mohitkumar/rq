package com.github.rq.queue;

import java.util.List;

public interface QueueOps {
    void leftPush(String queue, String message);

    void transferMessage(String fromQueue, String toQueue);

    String popMessage(String queue);

    String registerConsumer(String queueName, String consumer);

    void removeConsumer(String queueName, String consumer);

    List<String> getConsumers(String queueName);

    void copyList(String to, String from);
}

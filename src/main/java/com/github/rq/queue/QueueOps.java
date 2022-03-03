package com.github.rq.queue;

import java.util.List;

/**
 * <pre>
 * QueueOps is used by Queue and Consumer interface as a bridge to the underlying persistence implementation of various queue and consumer operations.
 *
 * </pre>
 *
 */
public interface QueueOps {
    /**
     * Push the message to queue (this operation is not blocking)
     * @param queue
     * @param message
     */
    void leftPush(String queue, String message);

    /**
     * Transfer message from the fromQueue to toQueue atomically and block until new message is avaliable in fromQueue
     * @param fromQueue
     * @param toQueue
     */
    void transferMessage(String fromQueue, String toQueue);

    /**
     * Pop message from queue in FIFO order, This operation is blocking.
     * @param queue
     * @return
     */
    String popMessage(String queue);

    /**
     * Register a consumer for a queue , it store this information  in persistence storage
     * This helps in making the consumer fault tolerance
     * @param queueName
     * @param consumer
     * @return
     */
    String registerConsumer(String queueName, String consumer);

    /**
     * Remove the already registered consumer
     * @param queueName
     * @param consumer
     */
    void removeConsumer(String queueName, String consumer);

    /**
     * Get all the consumer attached to the queue
     * @param queueName
     * @return
     */
    List<String> getConsumers(String queueName);

    void copyList(String to, String from);
}

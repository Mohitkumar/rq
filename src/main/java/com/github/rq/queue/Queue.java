package com.github.rq.queue;

import com.github.rq.Message;
import com.github.rq.serializer.MessageSerializer;

/**
 * Queue represents a FIFO queue
 * @param <T>
 */
public interface Queue<T> {

    /**
     * Get the name of the queue
     * @return
     */
    String getName();

    /**
     * Get the underlying QueueOps used for this queue
     * @return
     */
    QueueOps getQueueOps();

    /**
     * Enqueue the message in the queue
     * @param message
     */
    void enqueue(Message<T> message);

    /**
     * Dequeue the message available in the queue , If queue is empty this operation blocks the current thread until
     * New elements are available to dequeue
     * @return
     */
    Message<T> dequeue();

    /**
     * Get the underlying message serializer used to persist the message in queue
     * @return
     */
    MessageSerializer<T> getMessageSerializer();

    /**
     * Atomically pop the message from this queue and push it to the queue provided in paramter.
     * @param queue
     */
    void transferTo(Queue<T> queue);
}

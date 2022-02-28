package com.github.rq.consumer;

import com.github.rq.ConsumerListener;
import com.github.rq.Message;
import com.github.rq.RetryableException;
import com.github.rq.queue.Queue;
import com.github.rq.queue.RedisQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleThreadConsumer<T> implements Consumer {

    private static final Logger logger = LoggerFactory.getLogger(SingleThreadConsumer.class);

    private boolean start = true;

    private Queue<T> queue;

    private ConsumerListener<T> listener;

    public void stop(){
        this.start = false;
    }

    @Override
    public void start() {
        while(start){
            Message<T> message = this.queue.dequeue();
            try {
                listener.onMessage(message);
            } catch (RetryableException e) {
                logger.error("retry exception retrying message",e);
                this.queue.enqueue(message);
            }
        }
    }

    @Override
    public void init() {
        if(this.queue instanceof RedisQueue){
            ((RedisQueue)this.queue).inferType(listener.getClass(), ConsumerListener.class);
        }
    }
}

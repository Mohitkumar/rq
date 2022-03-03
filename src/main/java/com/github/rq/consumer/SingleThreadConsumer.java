package com.github.rq.consumer;

import com.github.rq.ConsumerListener;
import com.github.rq.Message;
import com.github.rq.RetryableException;
import com.github.rq.queue.Queue;
import com.github.rq.queue.RedisQueue;
import com.github.rq.retry.Retrier;
import com.github.rq.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single thread consumer
 * @param <T>
 */
public class SingleThreadConsumer<T> implements Consumer<T> {

    private static final Logger logger = LoggerFactory.getLogger(SingleThreadConsumer.class);

    private Queue<T> queue;

    private ConsumerListener<T> listener;

    private Retrier<T> retrier;

    private ConsumerThread consumerThread;

    public void stop(){
        consumerThread.stopRequested = true;
    }

    public SingleThreadConsumer(ConsumerListener<T> listener, Queue<T> queue, RetryPolicy retryPolicy) {
        this.queue = queue;
        this.listener = listener;

        retrier = new Retrier<>(retryPolicy,new RedisQueue<>(queue.getQueueOps(), queue.getMessageSerializer(), "retry"),queue);
        consumerThread = new ConsumerThread(() ->{
            Message<T> message = this.queue.dequeue();
            try {
                listener.onMessage(message, "single");
            } catch (RetryableException e) {
                boolean retry = retrier.retry(message);
                if(!retry){
                    logger.info("retry limit exhausted for message {}", message);
                }
            }
        });
    }

    @Override
    public void start() {
       consumerThread.start();
    }

    protected class ConsumerThread extends Thread{
        private boolean stopRequested = false;
        private Runnable callback;

        public ConsumerThread(Runnable callback) {
            this.callback = callback;
        }

        @Override
        public void run() {
            while (!stopRequested && !isInterrupted()) {
                try {
                    callback.run();
                } catch (Throwable t) {
                    logger.error("Exception while handling next queue item.", t);
                }
            }
        }
    }
}

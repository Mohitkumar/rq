package com.github.rq.consumer;

import com.github.rq.ConsumerListener;
import com.github.rq.Message;
import com.github.rq.queue.QueueOps;
import com.github.rq.queue.RedisOps;
import com.github.rq.RetryableException;
import com.github.rq.queue.Queue;
import com.github.rq.queue.RedisQueue;
import com.github.rq.retry.Retrier;
import com.github.rq.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * MultiThreadConsumer is capable of consuming the message from queue in multiple threads.
 * It calls the provided ConsumerListener on the consumed message.
 * @param <T>
 */
public class MultiThreadConsumer<T> implements Consumer<T>{
    private static  final Logger LOGGER = LoggerFactory.getLogger(MultiThreadConsumer.class);

    private static final long MAX_WAIT_MILLIS_WHEN_STOPPING_THREADS = 30000;

    private int numThreads;

    private ConsumerListener<T> listener;

    private Queue<T> queue;

    private List<ConsumerThread> consumerThreads ;

    private List<Queue<T>> redisQueues;

    private Retrier<T> retrier;

    private TransferThread transferThread;

    private QueueOps queueOps;

    public MultiThreadConsumer(int numThreads,
                               ConsumerListener<T> listener, Queue<T> queue, RetryPolicy retryPolicy) {
        this.numThreads = numThreads;
        this.listener = listener;
        this.queue = queue;
        consumerThreads = new ArrayList<>(numThreads);
        redisQueues = new ArrayList<>(numThreads);
        queueOps = queue.getQueueOps();
        retrier = new Retrier<>(retryPolicy,new RedisQueue<>(queueOps, queue.getMessageSerializer(), "retry"),queue);
    }

    public void init(){
        queue.inferType(listener.getClass(), ConsumerListener.class);
        List<String> oldConsumers = queueOps.getConsumers(queue.getName());
        List<String> newConsumers = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            String consumerName = queueOps.registerConsumer(this.queue.getName(), String.valueOf(i));
            RedisQueue<T> redisQueue = new RedisQueue<>(queueOps, queue.getMessageSerializer(), consumerName);
            redisQueue.inferType(listener.getClass(), ConsumerListener.class);
            redisQueues.add(redisQueue);
            newConsumers.add(consumerName);
        }
        Collections.sort(redisQueues, Comparator.comparing(Queue::getName));

        oldConsumers.removeAll(newConsumers);
        for (String oldConsumer : oldConsumers) {
            queueOps.copyList(this.queue.getName(),oldConsumer);
            queueOps.removeConsumer(this.queue.getName(),oldConsumer);
        }
    }

    @Override
    public void start() {
        init();
        transferThread = new TransferThread(redisQueues, queue);
        transferThread.start();
        retrier.start();
        for (int i = 0; i < redisQueues.size(); i++) {
            String consumerName = String.format("consumer-%s-%s", queue.getName(), i);
            Queue<T> transferQueue = redisQueues.get(i);
            ConsumerThread consumerThread =  new ConsumerThread(() ->{
                Message<T> message = transferQueue.dequeue();
                if(message != null){
                    try {
                        listener.onMessage(message, consumerName);
                    } catch (RetryableException e) {
                        boolean retry = retrier.retry(message);
                        if(!retry){
                            LOGGER.info("retry limit exhausted for message {}", message);
                        }
                    }
                }
            });
            consumerThread.setName(consumerName);
            consumerThread.start();
            consumerThreads.add(consumerThread);
            LOGGER.debug("Started message consumer thread {}", consumerThread.getName());
        }
    }


    @Override
    public void stop() throws InterruptedException{
        retrier.stop();
        transferThread.stopRequested = true;
        try {
            for (ConsumerThread consumerThread : consumerThreads) {
                LOGGER.debug("Stopping message consuming thread {}", consumerThread.getName());
                consumerThread.stopRequested = true;
            }
            waitForAllThreadsToTerminate();
        } finally {
            consumerThreads.clear();
        }
    }

    private void waitForAllThreadsToTerminate() throws InterruptedException{
        for (ConsumerThread consumerThread : consumerThreads) {
            try {
                consumerThread.join(MAX_WAIT_MILLIS_WHEN_STOPPING_THREADS);
            } catch (InterruptedException e) {
                LOGGER.warn(String.format("Unable to join thread [%s].", consumerThread.getName()));
                throw e;
            }
        }
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
                    LOGGER.error("Exception while handling next queue item.", t);
                }
            }
        }
    }

    protected class TransferThread extends Thread{

        List<Queue<T>> redisQueues;

        private Queue<T> queue;

        private boolean stopRequested = false;

        public TransferThread(List<Queue<T>> redisQueues, Queue<T> queue) {
            this.redisQueues = redisQueues;
            this.queue = queue;
        }

        @Override
        public void run() {

            int i =0;
            while(!stopRequested && !isInterrupted()){
                if(i == Integer.MAX_VALUE){
                    i = 0;
                }
                Queue<T> toQueue = redisQueues.get(i++ % redisQueues.size());
                queue.transferTo(toQueue);
            }
        }
    }
}

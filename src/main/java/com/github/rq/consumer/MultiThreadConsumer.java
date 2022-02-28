package com.github.rq.consumer;

import com.github.rq.ConsumerListener;
import com.github.rq.Message;
import com.github.rq.RedisOps;
import com.github.rq.RetryableException;
import com.github.rq.queue.Queue;
import com.github.rq.queue.RedisQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MultiThreadConsumer<T> implements Consumer<T>{
    private static  final Logger LOGGER = LoggerFactory.getLogger(MultiThreadConsumer.class);

    private static final long MAX_WAIT_MILLIS_WHEN_STOPPING_THREADS = 30000;

    private boolean initialized = false;


    private RedisOps redisOps;

    private int numThreads;

    private ConsumerListener<T> listener;

    private Queue<T> queue;

    private List<ConsumerThread> consumerThreads ;

    private List<Queue<T>> redisQueues;

    public MultiThreadConsumer(RedisOps redisOps, int numThreads, ConsumerListener<T> listener, Queue<T> queue) {
        this.redisOps = redisOps;
        this.numThreads = numThreads;
        this.listener = listener;
        this.queue = queue;
        consumerThreads = new ArrayList<>(numThreads);
        redisQueues = new ArrayList<>(numThreads);
    }

    public void init(){
        List<String> oldConsumers = redisOps.getConsumers(queue.getName());
        List<String> newConsumers = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            String consumerName = redisOps.registerConsumer(this.queue.getName(), String.valueOf(i));
            RedisQueue<T> redisQueue = new RedisQueue<>(redisOps, queue.getMessageSerializer(), consumerName);
            redisQueue.inferType(listener.getClass(), ConsumerListener.class);
            redisQueues.add(redisQueue);
            newConsumers.add(consumerName);
        }
        Collections.sort(redisQueues, Comparator.comparing(Queue::getName));

        oldConsumers.removeAll(newConsumers);
        for (String oldConsumer : oldConsumers) {
            redisOps.copyList(this.queue.getName(),oldConsumer);
            redisOps.removeConsumer(this.queue.getName(),oldConsumer);
        }
        initialized = true;
    }

    @Override
    public void start() {
        if(!initialized){
            throw new RuntimeException("can not start consumers, not initialized...");
        }
        new Thread(new TransferThread(redisQueues, queue)).start();

        for (int i = 0; i < redisQueues.size(); i++) {
            Queue<T> transferQueue = redisQueues.get(i);
            ConsumerThread consumerThread =  new ConsumerThread(() ->{
                Message<T> message = transferQueue.dequeue();
                if(message != null){
                    try {
                        listener.onMessage(message);
                    } catch (RetryableException e) {
                        LOGGER.error("retry exception retrying message",e);
                        this.queue.enqueue(message);
                    }
                }
            });
            consumerThread.setName(String.format("consumer-%s-%s", queue.getName(), i));
            consumerThread.start();
            consumerThreads.add(consumerThread);

            LOGGER.debug(String.format("Started message consumer thread [%s]", consumerThread.getName()));
        }
    }


    @Override
    public void stop() throws InterruptedException{
        try {
            for (ConsumerThread consumerThread : consumerThreads) {
                LOGGER.debug(String.format("Stopping message consuming thread [%s]", consumerThread.getName()));
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

    protected class TransferThread implements Runnable{

        List<Queue<T>> redisQueues;

        private Queue<T> queue;

        private boolean stop = false;

        public TransferThread(List<Queue<T>> redisQueues, Queue<T> queue) {
            this.redisQueues = redisQueues;
            this.queue = queue;
        }

        @Override
        public void run() {

            int i =0;
            while(!stop){
                if(i == Integer.MAX_VALUE){
                    i = 0;
                }
                Queue<T> toQueue = redisQueues.get(i++ % redisQueues.size());
                queue.transferTo(toQueue);
            }
        }
    }
}

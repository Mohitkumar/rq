package com.github.rq.retry;

import com.github.rq.Message;
import com.github.rq.queue.Queue;

public class Retrier<T> {

    private final RetryPolicy retryPolicy;

    private final Queue<T> retryQueue;

    private final Queue<T> transferQueue;

    private RetrierThread retrierThread;

    public Retrier(RetryPolicy retryPolicy, Queue<T> retryQueue, Queue<T> transferQueue) {
        this.retryPolicy = retryPolicy;
        this.retryQueue = retryQueue;
        this.transferQueue = transferQueue;
    }

    public boolean retry(Message<T> m){
        if(retryPolicy.shouldRetry(m.getRetryCount(), System.currentTimeMillis())){
            m.setRetryCount(m.getRetryCount()+1);
            retryQueue.enqueue(m);
            return true;
        }
        return false;
    }

    public void start(){
        retrierThread= new RetrierThread();
        retrierThread.setName("retrier");
        retrierThread.start();
    }

    public void stop(){
        retrierThread.stopRequested = true;
    }

    class RetrierThread extends Thread{
        private boolean stopRequested = false;

        @Override
        public void run() {
            while(!stopRequested && !isInterrupted()){
                retryQueue.transferTo(transferQueue);
            }
        }
    }
}

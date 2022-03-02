package com.github.rq.retry;

import com.github.rq.Message;
import com.github.rq.queue.Queue;

public class Retrier<T> {

    private RetryPolicy retryPolicy;

    private Queue<T> retryQueue;

    private Queue<T> transferQueue;

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
        Thread t = new RetrierThread();
        t.setName("retrier");
        t.start();
    }

    class RetrierThread extends Thread{
        private boolean stop = false;

        @Override
        public void run() {
            while(!stop){
                retryQueue.transferTo(transferQueue);
            }
        }
    }
}

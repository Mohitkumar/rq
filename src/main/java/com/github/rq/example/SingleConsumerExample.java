package com.github.rq.example;

import com.github.rq.consumer.Consumer;
import com.github.rq.consumer.SingleThreadConsumer;
import com.github.rq.producer.DefaultProducer;
import com.github.rq.producer.Producer;
import com.github.rq.queue.Queue;
import com.github.rq.queue.QueueOps;
import com.github.rq.queue.RedisOps;
import com.github.rq.queue.RedisQueue;
import com.github.rq.redis.IRedisClient;
import com.github.rq.redis.RedisClient;
import com.github.rq.retry.SimpleRetryPolicy;
import com.github.rq.serializer.JacksonMessageSerializer;
import com.github.rq.serializer.MessageSerializer;

public class SingleConsumerExample {
    public static void main(String[] args) {
        IRedisClient client = new RedisClient("localhost",6379);

        QueueOps redisOps = new RedisOps("single", client);

        MessageSerializer<Data> serializer = new JacksonMessageSerializer<>();
        Queue<Data> queue = new RedisQueue<>(redisOps,serializer,"queue");

        Producer<Data> producer = new DefaultProducer<>(queue);

        Consumer<Data> consumer = new SingleThreadConsumer<Data>((message, consumerName)->{
            System.out.println(consumerName +"----"+message.getPayload());
        },queue, new SimpleRetryPolicy(1));
        consumer.start();

        new Thread(() ->{
            for (int i = 0; i < 1; i++) {
                Data d = new Data();
                d.setField1("field"+i);
                d.setField2(i);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                producer.submit(d);
            }
        }).start();
    }

    static class Data{
        String field1;

        int field2;

        public String getField1() {
            return field1;
        }

        public void setField1(String field1) {
            this.field1 = field1;
        }

        public int getField2() {
            return field2;
        }

        public void setField2(int field2) {
            this.field2 = field2;
        }

        @Override
        public String toString() {
            return "Data{" +
                    "field1='" + field1 + '\'' +
                    ", field2=" + field2 +
                    '}';
        }
    }
}

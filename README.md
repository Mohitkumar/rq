# rq
Library for Queue implemented over Redis

- rq contains queue implementation based on redis data-structures.
- It uses redis list blocking operation. Consumers blocks until an element is present to consume.
- It has SingleThreadConsumer and MultiThreadConsumer implementations. MutliThreadConsumer is usefull in cases where you want to fan-out/load balance the messages.
- Message is serialized as json.

Examples-

Single Thread Consumer-
----------------------

``` java
     public static void main(String[] args) {
        IRedisClient client = new RedisClient("localhost",6379);

        RedisOps redisOps = new RedisOps("single", client);

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
```

Multi Thread Example--
---------------------

``` java
  public class MultiConsumerExample {

    public static void main(String[] args) {
        IRedisClient client = new RedisClient("localhost",6379);

        RedisOps redisOps = new RedisOps("example", client);

        MessageSerializer<Data>  serializer = new JacksonMessageSerializer<>();
        Queue<Data> queue = new RedisQueue<>(redisOps,serializer,"data-queue");

        Producer<Data> producer = new DefaultProducer<>(queue);

        Consumer<Data> consumer = new MultiThreadConsumer<>(4, new DataListener(),queue,
                new SimpleRetryPolicy(2));
        consumer.start();

        new Thread(() ->{
            for (int i = 0; i < 100; i++) {
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

    static class DataListener implements ConsumerListener<Data>{

        @Override
        public void onMessage(Message<Data> t, String consumerName) throws RetryableException {
            System.out.println(consumerName +"----"+t.getPayload());
            if(t.getPayload().getField2() == 5){
                throw new RetryableException("failed");
            }
        }
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
```


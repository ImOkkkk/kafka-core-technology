import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

/**
 * @author ImOkkkk
 * @date 2021/11/16 22:43
 * @since 1.0
 */
public class TestDemo {
    private KafkaConsumer<String, String> consumer;
    private ExecutorService executorService;
    private int workerNum = 20;


    public void MultithreadedConsume() {
        executorService = new ThreadPoolExecutor(workerNum, workerNum, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                executorService.submit(new Worker(record));
            }
        }
    }

    class Worker implements Runnable{
        private ConsumerRecord<String, String> record;

        public Worker(ConsumerRecord<String, String> record) {
            this.record = record;
        }

        @Override
        public void run() {
            // 执行消息处理逻辑
        }
    }

    @Test
    private void ProducerTest() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("acks", "all");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 开启GZIP压缩
        properties.put("compression.type", "gzip");
        List<String> interceptors = new ArrayList<>();
        // 配置拦截器
        interceptors.add("org.imokkkk.interceptors.AddTimestampInterceptor"); // 拦截器 1
        interceptors.add("org.imokkkk.interceptors.UpdateCounterInterceptor"); // 拦截器 2
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        // 开启幂等性
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        // 事务型Producer
        producer.initTransactions(); // 事务初始化
        try {
            ProducerRecord<String, String> record1 = new ProducerRecord<>("topic1", "k1", "v1");
            ProducerRecord<String, String> record2 = new ProducerRecord<>("topic1", "k2", "v2");
            producer.beginTransaction(); // 事务开始
            producer.send(record1, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // TODO
                }
            });
            producer.send(record2, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // TODO
                }
            });
            producer.commitTransaction(); // 事务提交
        } catch (KafkaException e) {
            producer.abortTransaction(); // 事务终止
        }
    }

    @Test
    private void ConsumerTest() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", true);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("foo", "bar"));
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//            // 处理消息
//            process(records);
            // 同步提交
//            try {
//                consumer.commitAsync();
//            } catch (CommitFailedException e) {
//                // 处理提交失败异常
//                handle(e);
//            }

            // 异步提交
//            consumer.commitAsync(((offsets, exception) -> {
//                if (exception != null){
//                    handle(exception);
//                }
//            }));

//            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
//            }
//        }
        // 同步提交、异步提交结合
//        try {
//            while (true){
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//                process(records);
//                // 使用异步提交规避阻塞
//                consumer.commitAsync();
//            }
//        }catch (Exception e){
//            // 处理异常
//            handle(e);
//        }finally {
//            try {
//                // 最后一次提交使用同步阻塞式提交
//                consumer.commitSync();
//            }finally {
//                consumer.close();
//            }
//        }
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            int count = 0;
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for (ConsumerRecord<String, String> record : records) {
                // 处理消息
                process(record);
                offsets.put(new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1));
                if (count % 100 == 0) {
                    consumer.commitAsync(offsets, null);
                }
                count++;
            }
        }
    }

    private void handle(Exception e) {
        
    }

    private void process(ConsumerRecords<String, String> records) {
        
    }

    private void process(ConsumerRecord<String, String> record) {

    }
    public class KafkaConsumerRunner implements Runnable{
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final KafkaConsumer consumer;

        public KafkaConsumerRunner(KafkaConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            try {
                consumer.subscribe(Arrays.asList("topic"));
                while (!closed.get()){
                    ConsumerRecords records = consumer.poll(Duration.ofMillis(10000));
                    // 执行消息处理逻辑
                }
            }catch (WakeupException e){
                if(!closed.get()){
                    throw e;
                }
            }finally {
                consumer.close();
            }
        }

        public void shutdown(){
            closed.set(true);
            consumer.wakeup();
        }
    }


    // Consumer 端 API 监控给定消费者组的 Lag 值
    public static Map<TopicPartition, Long> lagOf(String groupId, String boostrapServers) throws TimeoutException {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        try (AdminClient client = AdminClient.create(properties)) {
            // 调用AdminClient.listConsumerGroupOffsets()获取给定消费者组的最新消费信息的位移
            ListConsumerGroupOffsetsResult result = client.listConsumerGroupOffsets(groupId);
            try {
                Map<TopicPartition, OffsetAndMetadata> consumerOffsets =
                    result.partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
                properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties)) {
                    // 获取订阅分区的最新消息位移
                    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumerOffsets.keySet());
                    // 执行减法操作，获取lag值并封装进一个Map对象
                    return endOffsets.entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey(),
                        entry -> entry.getValue() - consumerOffsets.get(entry.getKey()).offset()));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // 处理中断异常
                return Collections.emptyMap();
            } catch (ExecutionException e) {
                // 处理ExecutionException
                return Collections.emptyMap();
            } catch (TimeoutException e) {
                throw new TimeoutException("Time out when getting lag for consumer group" + groupId);
            }
        }
    }
}

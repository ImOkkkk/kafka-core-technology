import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * @author ImOkkkk
 * @date 2021/11/16 22:43
 * @since 1.0
 */
public class TestDemo {

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
}

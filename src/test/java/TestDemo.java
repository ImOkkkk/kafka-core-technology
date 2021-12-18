import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
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
}

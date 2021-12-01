import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

/**
 * @author ImOkkkk
 * @date 2021/11/16 22:43
 * @since 1.0
 */
public class TestDemo {

    @Test
    private void ProducerTest(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers","127.0.0.1:9092");
        properties.put("acks","all");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // 开启GZIP压缩
        properties.put("compression.type","gzip");
        List<String> interceptors = new ArrayList<>();
        // 配置拦截器
        interceptors.add("org.imokkkk.interceptors.AddTimestampInterceptor"); // 拦截器 1
        interceptors.add("org.imokkkk.interceptors.UpdateCounterInterceptor"); // 拦截器 2
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        Producer<String,String> producer = new KafkaProducer<String, String>(properties);
    }
}

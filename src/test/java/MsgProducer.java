import com.alibaba.fastjson.JSON;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.imokkkk.pojo.Order;

/**
 * @author liuwy
 * @date 2023/3/4 20:02
 * @since 1.0
 */
public class MsgProducer {

    private final static String TOPIC_NAME = "test1";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024 * 1024 * 30);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        int sendCount = 5;
        CountDownLatch countDownLatch = new CountDownLatch(sendCount);
        for (int i = 0; i < sendCount; i++) {
            Order order = new Order(i, 100 + i, 1, 1000.0);
            //指定发送分区
            /*ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
              TOPIC_NAME, 0, order.getOrderId().toString(),
              JSON.toJSONString(order));*/
            //未指定分区
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
              TOPIC_NAME, order.getOrderId().toString(),
              JSON.toJSONString(order));
            //同步发送
            RecordMetadata metadata = producer.send(producerRecord).get();
            System.out.println("同步方式发送消息结果：" + "topic-" + metadata.topic() + "|partition-"
              + metadata.partition() + "|offset-" + metadata.offset());
            //异步发送
            /*producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception != null){
                        System.err.println("发送消息失败：" + exception.getStackTrace());
                    }
                    if(metadata != null){
                        System.out.println("异步方式发送消息结果：" + "topic-" + metadata.topic() + "|partition-"
                          + metadata.partition() + "|offset-" + metadata.offset());
                    }
                    countDownLatch.countDown();
                }
            });*/
        }
        countDownLatch.await();
        producer.close();
    }

}

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author liuwy
 * @date 2023/3/4 20:22
 * @since 1.0
 */
public class MsgConsumer {

    private final static String TOPIC_NAME = "test1";
    private final static String CONSUMER_GROUP_NAME = "testGroup";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
//        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10 * 1000);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30 * 1000);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        //消费指定分区
//        consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME, 0)));
        //消息回溯消费
//        consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME, 0)));
//        consumer.seekToBeginning(Arrays.asList(new TopicPartition(TOPIC_NAME, 0)));
        //指定offset消费
//        consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME, 0)));
//        consumer.seek(new TopicPartition(TOPIC_NAME, 0), 10);
        //从指定时间点开始消费
//        List<PartitionInfo> partitions = consumer.partitionsFor(TOPIC_NAME);
        //从1小时前消费
        /*long fetchDataTime = new Date().getTime() - 1000 * 60 * 60;
        Map<TopicPartition, Long> map = new HashMap<>();
        for (PartitionInfo partition : partitions) {
            map.put(new TopicPartition(TOPIC_NAME, partition.partition()), fetchDataTime);
        }
        Map<TopicPartition, OffsetAndTimestamp> parMap = consumer.offsetsForTimes(
          map);
        for (Entry<TopicPartition, OffsetAndTimestamp> entry : parMap.entrySet()) {
            TopicPartition key = entry.getKey();
            OffsetAndTimestamp value = entry.getValue();
            long offset = value.offset();
            System.out.println("partition-" + key.partition() + "|offset-" + offset);
            System.out.println();
            //根据消费里的timestamp确定offset
            if (value != null) {
                consumer.assign(Arrays.asList(key));
                consumer.seek(key, offset);
            }
        }*/

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("收到消息：partition = %d,offset = %d, key = %s, value = %s%n",
                  record.partition(),
                  record.offset(), record.key(), record.value());
            }
            if (records.count() > 0) {
                //手动同步提交offset，当前线程会阻塞直到offset提交成功
                consumer.commitSync();

                //手动异步提交offset，当前线程提交offset不会阻塞，可以继续处理后面的程序逻辑
//                consumer.commitAsync(new OffsetCommitCallback() {
//                    @Override
//                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
//                      Exception exception) {
//                        if (exception != null) {
//                            System.err.println("Commit failed for " + offsets);
//                            System.err.println(
//                              "Commit failed exception: " + exception.getStackTrace());
//                        }
//                    }
//                });
            }
        }
    }

}

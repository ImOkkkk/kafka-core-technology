package seek;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author ImOkkkk
 * @date 2022/9/2 10:19
 * @since 1.0
 */
public class SeekDemo {

  public void test(String boostrapServers, String groupId) {
    Properties properties = new Properties();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    String topic = "test"; // 要重设位移的 Kafka 主题
    try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(
        properties)) {
      consumer.subscribe(Collections.singleton(topic));
      // 重设位移
      // 1.消费者程序，要禁止自动提交位移；
      // 2.组 ID 要设置成你要重设的消费者组的组 ID；
      // 3.调用 seekToBeginning 方法时，需要一次性构造主题的所有分区对象；
      // 4.一定要调用带长整型的 poll 方法，而不要调用consumer.poll(Duration.ofSecond(0))。
      consumer.poll(0);
      consumer.seekToBeginning(consumer.partitionsFor(topic).stream().map(
              partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
          .collect(
              Collectors.toList()));

      //Latest
      consumer.seekToEnd(
          consumer.partitionsFor(topic).stream().map(partitionInfo -> new TopicPartition(topic,
              partitionInfo.partition())).collect(Collectors.toList()));

      //Current
      //借助 KafkaConsumer 的 committed 方法来获取当前提交的最新位移
      //首先调用 partitionsFor 方法获取给定主题的所有分区，然后依次获取对应分区上的已提交位移，最后通过 seek 方法重设位移到已提交位移处。
      consumer.partitionsFor(topic).stream().map(partitionInfo -> new TopicPartition(topic,
          partitionInfo.partition())).forEach(topicPartition -> {
        long committedOffset = consumer.committed(topicPartition).offset();
        consumer.seek(topicPartition, committedOffset);
      });

      //Specified-Offset
      long targetOffset = 1234L;
      for (PartitionInfo partitionInfo : consumer.partitionsFor(topic)) {
        TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
        consumer.seek(topicPartition, targetOffset);
      }

      //Shift-By-N
      for (PartitionInfo partitionInfo : consumer.partitionsFor(topic)) {
        TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
        //假设向前跳 123 条消息
        long targetOffsetL = consumer.committed(topicPartition).offset() + 123L;
        consumer.seek(topicPartition, targetOffsetL);
      }

      //DateTime
      long ts = LocalDateTime.of(2019, 6, 20, 20, 0).toInstant(ZoneOffset.ofHours(8))
          .toEpochMilli();
      Map<TopicPartition, Long> time2Search = consumer.partitionsFor(topic).stream()
          .map(partitionInfo -> new TopicPartition(topic,
              partitionInfo.partition()))
          .collect(Collectors.toMap(Function.identity(), topicPartition -> ts));
      for (Entry<TopicPartition, OffsetAndTimestamp> entry : consumer.offsetsForTimes(
          time2Search).entrySet()) {
        consumer.seek(entry.getKey(), entry.getValue().offset());
      }

      //Duration
      //将位移调回 30 分钟前
      Map<TopicPartition, Long> time2Search2 = consumer.partitionsFor(topic).stream()
          .map(partitionInfo -> new TopicPartition(topic,
              partitionInfo.partition())).collect(
              Collectors.toMap(Function.identity(),
                  topicPartition -> System.currentTimeMillis() - Duration.ofMinutes(30)
                      .toMillis()));
      for (Entry<TopicPartition, OffsetAndTimestamp> entry : consumer.offsetsForTimes(
          time2Search2).entrySet()) {
        consumer.seek(entry.getKey(), entry.getValue().offset());
      }
    }
  }
}

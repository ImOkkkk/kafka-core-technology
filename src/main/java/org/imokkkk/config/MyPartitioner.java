package org.imokkkk.config;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

/**
 * @author ImOkkkk
 * @date 2021/11/2 22:41
 * @since 1.0
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        // 随机策略
//        return ThreadLocalRandom.current().nextInt(partitions.size());
        // 按消息键保序策略
        return Math.abs(key.hashCode())%partitions.size();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

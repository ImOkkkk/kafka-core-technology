package org.imokkkk.interceptors;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import redis.clients.jedis.Jedis;

/**
 * @author ImOkkkk
 * @date 2021/12/2 22:26
 * @since 1.0
 */
public class AvgLatencyProducerInterceptor implements ProducerInterceptor {

    private Jedis jedis; // 省略 Jedis 初始化

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        jedis.incr("totalSentMessage");
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

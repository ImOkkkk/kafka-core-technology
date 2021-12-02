package org.imokkkk.interceptors;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import redis.clients.jedis.Jedis;

/**
 * @author ImOkkkk
 * @date 2021/12/2 22:36
 * @since 1.0
 */
public class AvgLatencyConsumerInterceptor implements ConsumerInterceptor<String, String> {

    private Jedis jedis; // 省略 Jedis 初始化

    @Override
    public ConsumerRecords onConsume(ConsumerRecords<String, String> records) {
        long latency = 0L;
        for (ConsumerRecord<String, String> record : records) {
            // 当前时间减去封装在消息中的创建时间
            latency += (System.currentTimeMillis() - record.timestamp());
        }
        // 这批消息总的端到端处理延时
        jedis.incrBy("totalLatency", latency);
        // 总延时
        long totalLatency = Long.parseLong(jedis.get("totalLatency"));
        // 总消息数
        long totalSentMsgs = Long.parseLong(jedis.get("totalSentMessage"));
        // 端到端消息的平均处理延时
        jedis.set("avgLatency", String.valueOf(totalLatency / totalSentMsgs));
        return records;
    }

    @Override
    public void close() {

    }

    @Override
    public void onCommit(Map offsets) {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

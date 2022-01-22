package org.imokkkk.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import cn.hutool.core.thread.ThreadUtil;

/**
 * @author ImOkkkk
 * @date 2022/1/22 21:02
 * @since 1.0
 */
@Configuration
public class ThreadPoolConfig {
    @Value("${executor.concurrentSize:20}")
    private Integer concurrentSize;


    @Bean("kafkaConsumerExecutor")
    public ExecutorService kafkaConsumerExecutor() {
        return new ThreadPoolExecutor(concurrentSize, concurrentSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
            ThreadUtil.newNamedThreadFactory("kafkaConsumerExecutor", false));
    }
}

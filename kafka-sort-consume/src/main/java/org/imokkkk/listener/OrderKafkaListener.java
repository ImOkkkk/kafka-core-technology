package org.imokkkk.listener;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.imokkkk.model.dto.OrderDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;

import cn.hutool.core.collection.CollUtil;

/**
 * @author ImOkkkk
 * @date 2022/1/22 21:24
 * @since 1.0
 */
@Component
public class OrderKafkaListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderKafkaListener.class);

    // private List<ConcurrentLinkedQueue<OrderDTO>> queues;

    @Value("${executor.concurrentSize:20}")
    private Integer concurrentSize;

    private AtomicLong pendingOffsets;

    private ConcurrentLinkedQueue<OrderDTO> orderDTOLinkedList;

    @Autowired
    @Qualifier("kafkaConsumerExecutor")
    private ExecutorService executorService;

    @KafkaListener(topics = {"${kafka.order.topic:order-test}"}, containerFactory = "commonListenerContainerFactory")
    public void consumerOrderMsg(List<ConsumerRecord<?, ?>> records, Acknowledgment acknowledgment) {
        if (!records.isEmpty()) {
            records.forEach(consumerRecord -> {
                OrderDTO orderDTO = JSON.parseObject(consumerRecord.value().toString(), OrderDTO.class);
                orderDTOLinkedList.offer(orderDTO);
            });
            while (CollUtil.isNotEmpty(orderDTOLinkedList)) {
                // OrderDTO orderDTO = orderDTOLinkedList.poll();
                // ConcurrentLinkedQueue<OrderDTO> queue = queues.get((int)(orderDTO.getId() % concurrentSize));
                // queue.offer(orderDTO);

                Future future = executorService.submit(new OrderConsumeTask(orderDTOLinkedList, pendingOffsets));
                try {
                    future.get();
                } catch (Exception e) {
                    throw new RuntimeException(e.getCause());
                }
            }

            while (true) {
                if (records.size() == pendingOffsets.get()) {
                    acknowledgment.acknowledge();
                    LOGGER.info("offset提交：{}", records.get(records.size() - 1).offset());
                    pendingOffsets.set(0L);
                    break;
                }
            }
        }
    }

    @PostConstruct
    public void init() {
        this.orderDTOLinkedList = new ConcurrentLinkedQueue<>();
        // this.queues = new ArrayList<>();
        pendingOffsets = new AtomicLong();
        /*for (int i = 0; i < this.concurrentSize; i++) {
            this.queues.add(new ConcurrentLinkedQueue<>());
        }*/
    }

    class OrderConsumeTask implements Callable {
        Logger logger = LoggerFactory.getLogger(OrderConsumeTask.class);

        // private List<ConcurrentLinkedQueue<OrderDTO>> queues;
        private ConcurrentLinkedQueue<OrderDTO> queues;

        private AtomicLong pendingOffsets;

        public OrderConsumeTask(ConcurrentLinkedQueue<OrderDTO> queues, AtomicLong pendingOffsets) {
            this.queues = queues;
            this.pendingOffsets = pendingOffsets;
        }

        @Override
        public Object call() throws Exception {
            // ConcurrentLinkedQueue<OrderDTO> currOrderQueue = queues
            // .get(Integer.parseInt(Thread.currentThread().getName().replaceAll("kafkaConsumerExecutor", "")) - 1);
            while (!queues.isEmpty()) {
                OrderDTO orderDTO = null;
                try {
                    orderDTO = queues.poll();
                    if (orderDTO != null) {
                        logger.info("线程：{}，执行任务：{}，成功！", Thread.currentThread().getName(), JSON.toJSONString(orderDTO));
                        pendingOffsets.incrementAndGet();
                    }
                } catch (Exception e) {
                    logger.error("线程：{}执行失败！订单：{}处理失败！异常信息：{}", Thread.currentThread().getName(),
                        JSON.toJSONString(orderDTO), e);
                }
            }
            return null;
        }
    }
}

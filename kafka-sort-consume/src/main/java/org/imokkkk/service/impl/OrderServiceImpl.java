package org.imokkkk.service.impl;

import org.imokkkk.model.dto.OrderDTO;
import org.imokkkk.service.OrderService;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import cn.hutool.core.util.RandomUtil;

/**
 * @author ImOkkkk
 * @date 2022/1/22 21:18
 * @since 1.0
 */
@Service
public class OrderServiceImpl implements OrderService {
    @Override
    @Retryable(Exception.class)
    public void solveRetry(OrderDTO order) throws InterruptedException {
        // TODO
        Thread.sleep(50);
        // 模拟异常
        if (ObjectUtils.nullSafeEquals(order.getId(), RandomUtil.randomInt(0, 100))) {
            throw new RuntimeException("模拟订单id为" + order.getId() + "的数据报错！");
        }
    }
}

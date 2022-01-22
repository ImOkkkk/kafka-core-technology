package org.imokkkk.service;

import org.imokkkk.model.dto.OrderDTO;

/**
 * @author ImOkkkk
 * @date 2022/1/22 21:16
 * @since 1.0
 */
public interface OrderService {

    /**
     * 处理订单数据
     * @param order
     */
    void solveRetry(OrderDTO order) throws InterruptedException;
}

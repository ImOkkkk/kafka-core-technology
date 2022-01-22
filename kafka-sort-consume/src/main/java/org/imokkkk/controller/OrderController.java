package org.imokkkk.controller;

import org.imokkkk.model.dto.OrderDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSON;

/**
 * @author ImOkkkk
 * @date 2022/1/22 22:21
 * @since 1.0
 */

@RestController
@RequestMapping("/order")
public class OrderController {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @GetMapping("/send")
    public void send() {
        for (long i = 0; i < 100; i++) {
            OrderDTO order = new OrderDTO();
            order.setOrderName("订单" + i);
            order.setId(i);
            kafkaTemplate.send("order-test", JSON.toJSONString(order));
        }
    }
}

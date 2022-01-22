package org.imokkkk.model.dto;

import lombok.Data;

/**
 * @author ImOkkkk
 * @date 2022/1/22 21:14
 * @since 1.0
 */
@Data
public class OrderDTO {

    /**
     * 订单id
     */
    private Long id;

    /**
     * 订单状态
     * 0：生成订单
     * 1：支付订单
     * 2：归档订单
     */
    private Integer status;

    /**
     * 订单名称
     */
    private String orderName;
}

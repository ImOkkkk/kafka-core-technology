package org.imokkkk.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author liuwy
 * @date 2023/3/4 20:01
 * @since 1.0
 */
@Data
@AllArgsConstructor
public class Order {

    private Integer orderId;
    private Integer productId;
    private Integer productNum;
    private Double orderAmount;
}

package org.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class OrderLog {

    public Long orderId;

    //訂單狀態 create pay
    public String OrderStatus;
    public static final String OrderStatusCreate= "create";
    public static final String OrderStatusPay = "pay";

    //三方回傳支付Id
    public String paymentId;

    //時間戳 必須確認是不是毫秒(Flink窗口指定毫秒)
    private Long timestamp;






}

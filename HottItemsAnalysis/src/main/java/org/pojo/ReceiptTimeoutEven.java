package org.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ReceiptTimeoutEven {

    private Long orderId;

    //三方回傳支付Id
    private String paymentId;

    //時間戳 必須確認是不是毫秒(Flink窗口指定毫秒)
    private Long timestamp;

    private boolean isPay;

    private boolean isReceipt;




}

package org.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ReceiptLog {

    //三方回傳支付Id
    public String paymentId;

    //支付方式 PayFun
    public String PayFun;
    public static final String wechat= "wechat";
    public static final String alipay = "alipay";

    //時間戳 必須確認是不是毫秒(Flink窗口指定毫秒)
    private Long timestamp;



}

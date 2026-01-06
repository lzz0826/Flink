package org.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class OrderTimeoutEvent {

    private Long orderId;

    //訂單窗創建時間 時間戳 必須確認是不是毫秒(Flink窗口指定毫秒)
    private Long createTimestamp;

    //訂單超時時間 時間戳 必須確認是不是毫秒(Flink窗口指定毫秒)
    private Long timeOutTimestamp;

    //訂單狀態 create pay
    public String OrderStatus = "TimeOut";


}

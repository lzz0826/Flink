package org.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AdClickLogAlarm {

    //使用者ID
    private Long userId;

    //廣告ID
    private Long advertiseId;

    //時間戳 必須確認是不是毫秒(Flink窗口指定毫秒)
    private Long timestamp;


}

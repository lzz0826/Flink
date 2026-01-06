package org.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.pojo.Enums.UserBehaviorEnum;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
//UserBehavior 對應數據 UserBehavior.csv
//543462,1715,1464116,pv,1511658000
public class UserBehavior {

    //userId
    private Long userId;

    //商品ID
    private Long itemId;

    //類別Id
    private Integer categoryId;

    //用戶行為
    private UserBehaviorEnum behavior;

    //字傳形式的 behavior 用於解決 Flink SQL 的映射問題
    private String behaviorStr;

    //時間戳 必須確認是不是毫秒(Flink窗口指定毫秒)
    private Long timestamp;

}




package org.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class LoginLog {

    //使用者ID
    private Long userId;

    //ip
    private String ip;

    //登入狀態 success fail
    private String loginStatus;
    public static final String LoginStatusSuccess = "success";
    public static final String LoginStatusFail = "fail";

    //時間戳 必須確認是不是毫秒(Flink窗口指定毫秒)
    private Long timestamp;




}

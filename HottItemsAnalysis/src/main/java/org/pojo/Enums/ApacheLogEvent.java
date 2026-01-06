package org.pojo.Enums;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ApacheLogEvent {

    //訪問的ip
    private String ip ;

    //訪問的userId
    private Long userId ;

    //訪問的時間
    private Long eventTime ;

    //訪問的方法 GET POST PUT DELETE
    private String method ;

    //訪問的url
    private String url ;


}

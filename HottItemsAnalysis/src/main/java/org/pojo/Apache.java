package org.pojo;


//數據格式
//83.149.9.216 - - 17/05/2015:10:05:47 +0000 GET /presentations/logstash-monitorama-2013/plugin/highlight/highlight.js

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@Data
@ToString
public class Apache {

    //IP
    String ip;

    // ident 代表客戶端的 identd 用戶識別。 現在幾乎沒人在用，Apache 也通常不啟用 ident lookup，所以永遠是 -。
    public String ident;

    // authUser 如果網站有基本認證（HTTP Basic Auth），這裡會顯示登入帳號 -代表 沒有登入（匿名存取）。
    public String authUser;

    //日誌記錄時間
    String logTime;

    //時區
    String timeZone;

    //日誌記錄時間 (轉換後的 時間搓)
    Long timestamp;

    //HTTP 協議
    String httpProtocol;

    //Url
    String url;


}

package org.pojo;

import java.util.HashSet;
import java.util.Set;

public class PvUvAccumulator {
    public Long pvCount = 0L;
    // 使用 Set 結構來存儲 UserId，實現自動去重 (UV)
    //UV:在每小時同一個用戶瀏覽只算一次
    public Set<Long> userIds = new HashSet<>();

    // 建議添加無參構造函數
    public PvUvAccumulator() {}
}


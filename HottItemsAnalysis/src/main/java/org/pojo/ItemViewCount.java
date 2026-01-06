package org.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ItemViewCount {

    //商品id
    private Long itemId;

    //窗口結束時間
    private Long windowEnd;

    //記數
    private Long count;


}

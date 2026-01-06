package org.pojo;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AdStatisticsByProvinceProcess {


    //省
    private String provinceId;

    //廣告ID . Count
    private Map<Long,Long> adCount = new HashMap<Long,Long>();

    //窗口結束時間
    private Long windowEnd;

}

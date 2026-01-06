package org.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AdStatisticsByProvinceAccumulator {

    //廣告ID . Count
    private Map<Long,Long> adCount = new HashMap<Long,Long>();


}

package org.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class HotUrlCount {


    private String ip;

    //窗口結束時間
    private Long windowEnd;

    //記數
    private Long ipCount;


    //url list
    private List<String> urlList;
}


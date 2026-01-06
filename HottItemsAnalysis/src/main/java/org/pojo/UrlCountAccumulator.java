package org.pojo;

import lombok.Data;

import java.util.HashMap;

@Data
// AggregateFunction 的 Accumulator (儲存每個 IP 訪問的 URL 次數 Map 和總訪問量)
public class UrlCountAccumulator {
    // Map<URL, Count>
    public HashMap<String, Long> urlCounts = new HashMap<>();
    // 該 IP 的總訪問量
    public Long totalIpAccessCount = 0L; // 該 IP 的總訪問量
}

package org.analysis.tableSQLAPI;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.conf.AnalysisConfig;
import org.pojo.AdClickLog;
import org.utils.FlinkSource;

import static org.apache.flink.table.api.Expressions.$;
import static org.conf.ConfigLoader.loadAppConfig;

// 統計 每小時  按照省份劃分 每個廣告點擊次數
public class AdStatisticsByProvinceSQL {


    public static void main(String[] args) throws Exception {

        AnalysisConfig config = loadAppConfig();

        int parallelism = config.adStatisticsByProvinceAnalysis.flink.parallelism;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parallelism);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String testData = config.adStatisticsByProvinceAnalysis.testData;
        DataStream<String> fileSource = FlinkSource.getFileSource(env, testData);

        DataStream<AdClickLog> parsed = fileSource.map(new MapFunction<String, AdClickLog>() {
            @Override
            public AdClickLog map(String line) throws Exception {

                String[] split = line.split(",");

                AdClickLog adClickLog = new AdClickLog();
                adClickLog.setUserId(Long.parseLong(split[0]));
                adClickLog.setAdvertiseId(Long.parseLong(split[1]));
                adClickLog.setProvince(split[2]);
                adClickLog.setCity(split[3]);
                // **目前時間戳是 1511658000 必須*1000 變為毫秒
                adClickLog.setTimestamp(Long.parseLong(split[4]) * 1000L);
                return adClickLog;
            }
        });

        DataStream<AdClickLog> withWatermark =
                parsed.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<AdClickLog>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );


        // 4. 將 DataStream 註冊為視圖 (View)
        // 注意：$("timestamp").rowtime() 會將這個欄位標記為 Event Time 屬性
        tableEnv.createTemporaryView(
                "ad_clickLog",
                withWatermark,
                $("userId"),
                $("advertiseId"),
                $("province"),
                $("city"),
                $("timestamp").rowtime().as("ts") // 這裡將 timestamp 欄位映射為 SQL 中的 'ts' 並設為事件時間
        );

        // 新版 TUMBLE(TABLE ad_clickLog, DESCRIPTOR(ts), INTERVAL '1' HOUR)
        // 邏輯：TUMBLE 窗口 (1小時) -> Group By (窗口, 省份, 廣告ID) -> Count
//        String sql =
//                "SELECT " +
//                        "  window_end, " +
//                        "  province, " +
//                        "  advertiseId, " +
//                        "  COUNT(*) as click_count " +
//                        "FROM TABLE( " +
//                        "  TUMBLE(TABLE ad_clickLog, DESCRIPTOR(ts), INTERVAL '1' HOUR) " +
//                        ") " +
//                        "GROUP BY window_start, window_end, province, advertiseId";

//        String sql =
//                "SELECT " +
//                        "  province, " +
//                        "  advertiseId, " +
//                        "  TUMBLE_END(ts, INTERVAL '1' HOUR) AS window_end, " + // 使用 TUMBLE_END 獲取窗口結束時間
//                        "  COUNT(*) as click_count " +
//                        "FROM " +
//                        "  ad_clickLog " +
//                        "GROUP BY " +
//                        "  TUMBLE(ts, INTERVAL '1' HOUR), " + // 在 GROUP BY 裡定義窗口
//                        "  province, " +
//                        "  advertiseId";

        String sql =
                "SELECT\n" +
                        "  t2.window_end,\n" +
                        "  t2.province,\n" +
//                        "  -- 使用 COLLECT 函數將所有 '廣告ID:點擊數' 的字串收集成一個列表\n" +
                        "  COLLECT(\n" +
                        "      CAST(t2.advertiseId AS STRING) || ':' || CAST(t2.click_count AS STRING)\n" +
                        "  ) AS advertiseId_counts\n" +
                        "FROM (\n" +
//                        "    -- 內層查詢：先計算每個窗口內，省份和廣告ID的點擊次數\n" +
                        "    SELECT \n" +
                        "      window_end, \n" +
                        "      province, \n" +
                        "      advertiseId, \n" +
                        "      COUNT(*) as click_count \n" +
                        "    FROM TABLE( \n" +
                        "      TUMBLE(TABLE ad_clickLog, DESCRIPTOR(ts), INTERVAL '1' HOUR) \n" +
                        "    ) \n" +
                        "    GROUP BY window_start, window_end, province, advertiseId\n" +
                        ") AS t2\n" +
//                        "-- 外層分組：只按窗口結束時間和省份分組，將所有廣告數據彙集\n" +
                        "GROUP BY \n" +
                        "  t2.window_end, \n" +
                        "  t2.province";


        Table resultTable = tableEnv.sqlQuery(sql);

        tableEnv.toChangelogStream(resultTable)
                .print("SQL-Result (Retract Mode)");

        env.execute("AdStatisticsByProvinceSQL");

    }


}

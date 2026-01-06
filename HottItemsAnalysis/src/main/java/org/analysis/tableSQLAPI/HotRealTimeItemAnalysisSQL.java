package org.analysis.tableSQLAPI;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.conf.AnalysisConfig;
import org.pojo.Enums.UserBehaviorEnum;
import org.pojo.UserBehavior;
import org.utils.FlinkSource;
import org.utils.KafkaTopicUtil;

import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.Expressions.*; // 用於 Table API 表達式
import static org.conf.ConfigLoader.loadAppConfig;


//無法使用 DataStreamAPI的
//定時器
//手動狀態控制
//底層上下文訪問
//自定義 Window 觸發器
//側邊輸出
//自定義數據源/接收器
public class HotRealTimeItemAnalysisSQL {

    public static void main(String[] args) throws Exception {

        AnalysisConfig config = loadAppConfig();
        String topic = config.hotRealTimeItemAnalysis.kafka.topic;
        String group = config.hotRealTimeItemAnalysis.kafka.group;
        String host = config.hotRealTimeItemAnalysis.kafka.host;
        String port = config.hotRealTimeItemAnalysis.kafka.port;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // **1. 建立 StreamTableEnvironment**
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //建立 Kafka Source
        DataStream<String> kafkaSource = FlinkSource.getKafkaSource(env,host,port,topic,group,1);

        // 轉換成物件
        DataStream<UserBehavior> parsed = kafkaSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String line) throws Exception {

                String[] split = line.split(",");

                UserBehavior userBehavior = new UserBehavior();

                userBehavior.setUserId(Long.parseLong(split[0]));
                userBehavior.setItemId(Long.parseLong(split[1]));
                userBehavior.setCategoryId(Integer.parseInt(split[2]));
                userBehavior.setBehaviorStr(split[3]);
                //** Flink SQL 不能用 enum
                userBehavior.setBehavior(UserBehaviorEnum.getByValue(split[3]));
                //**目前時間戳是 1511658000 必須*1000 變為毫秒
                userBehavior.setTimestamp(Long.parseLong(split[4]) * 1000L);
                return userBehavior;
            }
        });


        // **2. 處理 Watermark 和時間欄位**
        // 在 Table API/SQL 中，必須有一個 'rowtime' 屬性的時間欄位。
        // 代碼中的 UserBehavior.timestamp (Long) 需轉換為 SQL TIMESTAMP。
        DataStream<UserBehavior> withWatermark =
                parsed.assignTimestampsAndWatermarks(
                        // 使用 forMonotonousTimestamps() 策略，適用於嚴格升序的數據
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );
//                parsed.assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                // 使用 forBoundedOutOfOrderness() 亂序策略
//                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(2)) // 允許亂序 2 秒
//                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
//                );



        // **3. 註冊 Table**
        // 定義 Table Schema 並指定事件時間欄位 (ts)，以便後續進行 Event Time Window 操作
        // AS T_TIME.ROWTIME() 定義了事件時間欄位
        tableEnv.createTemporaryView(
                "user_behavior",
                withWatermark,
                $("userId"),
                $("itemId"),
                $("categoryId"),
                //處理 enum
                $("behaviorStr"),
                // 將 timestamp 欄位定義為 Flink 的行時間 (Rowtime)，作為 Watermark 基準
                $("timestamp").rowtime().as("ts")// Flink 知道這是事件時間欄位，但類型仍是 BIGINT
        );

//        HOP(),分組 (Grouping),GROUP BY,必須與 HOP_END() 相同。
//        HOP_END(),輸出 (Output),SELECT,必須與 HOP() 相同。

//        FlinkSQL自動生成欄位名稱,            數據類型,                    說明,                      用途
//        window_start,TIMESTAMP_LTZ(3)。   該窗口的起始時間（包含）。      必須用於 GROUP BY 子句      作為窗口的分組鍵之一。
//        window_end,TIMESTAMP_LTZ(3)       該窗口的結束時間（不包含）。    必須用於 GROUP BY 子句；    常用於 SELECT 中作為窗口結束標記。
//        window_time,TIMESTAMP_LTZ(3)      聚合結果的時間戳。在多數情況下， 它等同於 window_end 欄位。  可選用於 SELECT，或作為下游 Watermark 的時間戳。

        // **4. 實現 SQL 邏輯 (分為兩階段)**

        // 階段一：窗口聚合 (Window Aggregate)
        // 篩選出 PV 行為，並在 1 小時窗口、5 分鐘滑動的窗口內按 itemId 進行計數。
        // HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) 定義了滑動窗口 (Slide, Size) 必須 GROUP BY
        // HOP_END(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)取得窗口結束時間
        String windowAggSql =
                "SELECT " +
                        "  itemId, " +
                        "  COUNT(itemId) AS view_count, " +
                        "  HOP_END(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) AS window_end " +
                        "FROM " +
                        "  user_behavior " +
                        "WHERE " +
                        "  behaviorStr = 'pv' " + // 僅統計 PV 行為
                        "GROUP BY " +
                        "  itemId, " +
                        "  HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)";

        Table aggTable = tableEnv.sqlQuery(windowAggSql);

        // 註冊聚合結果為臨時表
        tableEnv.createTemporaryView("item_view_count", aggTable);

        // 階段二：窗口內 Top N 排序 (Top N Ranking)
        // 使用 ROW_NUMBER() 函數在每個窗口 (window_end) 內對 view_count 進行排序並選取 Top 5
        String rankSql =
                "SELECT * " +
                        "FROM (" +
                        "  SELECT " +
                        "    *, " +
                        "    ROW_NUMBER() OVER (" +
                        "      PARTITION BY window_end " + // 按照窗口結束時間分組
                        "      ORDER BY view_count DESC " + // 按瀏覽次數降序排列
                        "    ) AS row_num " + // 計算排名
                        "  FROM item_view_count " +
                        ") " +
                        "WHERE row_num <= 5"; // 篩選出 Top 5

        Table resultTable = tableEnv.sqlQuery(rankSql);

        // **5. 結果輸出**
        // 將 Table 轉換回 DataStream 進行打印或後續處理
        // Row.toString() 輸出的格式會包含 Row 的類型資訊，例如 +I[1715, 10, 1511661600000, 1]
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

        // 您可以自己寫一個 MapFunction 格式化輸出
        resultStream.print("HotRealTimeItemTopN_SQL");

        env.execute("Hot Real Time Item Analysis (SQL)");

    }
}
package org.analysis.tableSQLAPI;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.conf.AnalysisConfig;
import org.pojo.Enums.UserBehaviorEnum;
import org.pojo.PvUvAccumulator;
import org.pojo.PvUvCount;
import org.pojo.UserBehavior;
import org.utils.AnalysisFileInputFormat;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.conf.ConfigLoader.loadAppConfig;

//PV UV統計

//需求:
//從埋點日誌中 統計實時的 PV UV
//統計每小時訪問量(PV 頁面瀏覽量) 並且對用戶進行去重(UV 使用者瀏覽量 要去重 UV:在每小時同一個用戶瀏覽只算一次 )

//思路:
//統計埋點日誌中的pv行為 利用Set數據結構去重
//對於超貸規模的數據 可以考慮用布隆過濾器去重

//數據格式:543462,1715,1464116,pv,1511658000
//送訊息
//docker exec -it kafka kafka-console-producer \
//        --broker-list localhost:9092 \
//        --topic PvUvAnalysisAnalysis_topic
public class PvUvAnalysisAnalysisSQL {

    public static void main(String[] args) throws Exception {

        AnalysisConfig config = loadAppConfig();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        String path = config.pbvUvAnalysisAnalysis.testData;

        FileInputFormat<String> input = new AnalysisFileInputFormat(new Path(path));
        // 讀檔
        DataStream<String> source = env.readFile(input, path);



        // 轉換成物件
        DataStream<UserBehavior> parsed = source.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String line) throws Exception {

                String[] split = line.split(",");

                UserBehavior userBehavior = new UserBehavior();

                userBehavior.setUserId(Long.parseLong(split[0]));
                userBehavior.setItemId(Long.parseLong(split[1]));
                userBehavior.setCategoryId(Integer.parseInt(split[2]));
                userBehavior.setBehaviorStr(split[3]);
                userBehavior.setBehavior(UserBehaviorEnum.getByValue(split[3]));
                // **目前時間戳是 1511658000 必須*1000 變為毫秒
                userBehavior.setTimestamp(Long.parseLong(split[4]) * 1000L);
                return userBehavior;
            }
        });

        // 設定 事件時間欄位 Timestamp + Watermark (使用新的 API)
        DataStream<UserBehavior> withWatermark =
                parsed.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(2)) // 允許亂序 2 秒
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()) // 事件時間欄位
                );

        //創建表
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

        //滾動窗口 沒有重疊或間隔
        //TUMBLE_END(ts, INTERVAL '1' HOUR) AS window_end
        //TUMBLE(ts, INTERVAL '1' HOUR) -- 這裡只需要定義窗口長度 (1 小時)
        String windowAggSql = "SELECT\n" +
                "    COUNT(*) AS pvCount,\n" +
                "       COUNT(DISTINCT userId) AS uvCount,\n" +
                "    \n" +
                "    TUMBLE_END(ts, INTERVAL '1' HOUR) AS window_end\n" +
                "FROM\n" +
                "    user_behavior\n" +
                "WHERE\n" +
                "    behaviorStr = 'pv'\n" +
                "GROUP BY \n" +
                "    TUMBLE(ts, INTERVAL '1' HOUR)\n" +
                ";\n";

        Table aggTable = tableEnv.sqlQuery(windowAggSql);

        DataStream<Row> resultStream = tableEnv.toChangelogStream(aggTable);

        resultStream.print("PvUvAnalysisAnalysisSQL");

        env.execute("PvUv Analysis Job");
    }

}
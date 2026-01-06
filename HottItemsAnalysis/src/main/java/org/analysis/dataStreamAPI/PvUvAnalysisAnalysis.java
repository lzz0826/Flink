package org.analysis.dataStreamAPI;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.conf.AnalysisConfig;
import org.pojo.Enums.UserBehaviorEnum;
import org.pojo.PvUvAccumulator;
import org.pojo.PvUvCount;
import org.pojo.UserBehavior;
import org.utils.FlinkSource;

import java.time.Duration;

import static org.conf.ConfigLoader.loadAppConfig;

//PV UV統計

//需求:
//從埋點日誌中 統計實時的 PV UV
//統計每小時訪問量(PV 頁面瀏覽量) 並且對用戶進行去重(UV 使用者瀏覽量 要去重 UV:在每小時同一個用戶瀏覽只算一次 )

//思路:
//統計埋點日誌中的pv行為 利用Set數據結構去重
//對於超貸規模的數據 可以考慮用布隆過濾器去重

//展示: Aggregate累加, TumblingEventTimeWindows事件時給窗口, Set實現跨分區全局去重

//數據格式:543462,1715,1464116,pv,1511658000
//送訊息
//docker exec -it kafka kafka-console-producer \
//        --broker-list localhost:9092 \
//        --topic PvUvAnalysisAnalysis_topic
public class PvUvAnalysisAnalysis {

    //設置分區數。應與 Flink 並行度一致或略大。

    public static void main(String[] args) throws Exception {

        AnalysisConfig config = loadAppConfig();

        Long allowedLateness = config.pbvUvAnalysisAnalysis.flink.allowedLateness;
        String topic = config.pbvUvAnalysisAnalysis.kafka.topic;
        String group = config.pbvUvAnalysisAnalysis.kafka.group;
        String host = config.pbvUvAnalysisAnalysis.kafka.host;
        String port = config.pbvUvAnalysisAnalysis.kafka.port;
        int parallelism = config.pbvUvAnalysisAnalysis.flink.parallelism;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parallelism);

        //建立 Kafka Source
//        DataStream<String> kafkaSource = FlinkSource.getKafkaSource(env,host,port,topic,group,parallelism);

        String testData = config.pbvUvAnalysisAnalysis.testData;
        DataStream<String> fileSource = FlinkSource.getFileSource(env, testData);

        // 轉換成物件
        DataStream<UserBehavior> parsed = fileSource.map(new MapFunction<String, UserBehavior>() {
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

        // 過濾出 PV
        SingleOutputStreamOperator<UserBehavior> pvStream = withWatermark.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return value.getBehavior().equals(UserBehaviorEnum.PV);
            }
        });

        // KeyBy: 使用常量 Key，將所有數據導向同一個實例，以便進行總 PV 和總 UV 統計
        // 使用確定的 Hash 取模，將負載分散到 parallelism 個 Task。
        KeyedStream<UserBehavior, String> keyedStream = pvStream.keyBy(new KeySelector<UserBehavior, String>() {
            @Override
            public String getKey(UserBehavior value) throws Exception {
                // 確定性隨機：使用 userId 的 HashCode 進行取模
                // Flink 內部會使用 hashCode()，這裡只是模擬分組
                long keyHash = value.getUserId().hashCode();
                int keyIndex = (int) (keyHash % parallelism);

                // 確保 keyIndex 是非負數
                if (keyIndex < 0) {
                    keyIndex += parallelism;
                }
                // KeySelector 必須返回一個確定的 Key
                return "PvUvKey_Partial_" + keyIndex; // 第一階段局部 Key
            }
        });

        // 遲到資料的 OutputTag
        final OutputTag<UserBehavior> LATE_TAG = new OutputTag<UserBehavior>("Late-PvUvAnalysisAnalysis"){};

        // --- 第一階段：局部聚合（分散熱鍵負載） ---
        //這裡只使用 AggregateFunction，輸出 PvUvAccumulator (包含 Set)
        SingleOutputStreamOperator<PvUvAccumulator> localAggregate = keyedStream
                // 事件時間滾動窗口**，1 小時滾動計算一次
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .allowedLateness(Duration.ofSeconds(allowedLateness)) // 允許延遲
                .sideOutputLateData(LATE_TAG)
                // 僅使用 AggregateFunction 進行增量計算，輸出 PvUvAccumulator (包含 Set)
                .aggregate(new PvUvAnalysisAnalysisAggregateFunction());

        // --- 第二階段：全局聚合（Set 合併與最終輸出） ---
        SingleOutputStreamOperator<PvUvCount> globalAggregate = localAggregate
                // KeyBy：使用常數 Key，將所有局部 PvUvAccumulator 導向單一 Task 實例
                .keyBy(new KeySelector<PvUvAccumulator, String>() {
                    @Override
                    public String getKey(PvUvAccumulator value) throws Exception {
                        // 【加強註解】全局 Key，用於收集和合併所有分區的 Set
                        return "FinalGlobalKey";
                    }
                })
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                // 1. GlobalPvUvAnalysisAggregateFunction 負責合併 Set
                // 2. PvUvAnalysisProcessWindowFunction 負責格式化最終輸出
                .aggregate(new GlobalPvUvAnalysisAggregateFunction(), new PvUvAnalysisProcessWindowFunction());

        globalAggregate.print("PvUvAnalysisAnalysis Result (Global)");

        env.execute("PvUv Analysis Job");

    }

    // AggregateFunction: 同時累加 PV 和去重 UV
    // 輸入: UserBehavior | 累加器: PvUvAccumulator | 輸出: PvUvAccumulator (供 ProcessWindowFunction 使用)
    public static class PvUvAnalysisAnalysisAggregateFunction implements AggregateFunction<UserBehavior, PvUvAccumulator, PvUvAccumulator> {

        @Override
        public PvUvAccumulator createAccumulator() {
            return new PvUvAccumulator();
        }

        @Override
        public PvUvAccumulator add(UserBehavior value, PvUvAccumulator accumulator) {
            accumulator.pvCount += 1; // 累加 PV
            accumulator.userIds.add(value.getUserId()); // 使用 Set 去重 UV
            return accumulator;
        }

        @Override
        public PvUvAccumulator getResult(PvUvAccumulator accumulator) {
            return accumulator; // 返回包含 PV 總數和 UV Set 的累加器
        }

        @Override
        public PvUvAccumulator merge(PvUvAccumulator a, PvUvAccumulator b) {
            a.pvCount += b.pvCount;
            a.userIds.addAll(b.userIds);
            return a;
        }
    }

    // 第二階段 AggregateFunction: 合併多個 PvUvAccumulator (全局)
    // 輸入: PvUvAccumulator (局部結果) | 累加器/輸出: PvUvAccumulator (全局累計)
    public static class GlobalPvUvAnalysisAggregateFunction implements AggregateFunction<PvUvAccumulator, PvUvAccumulator, PvUvAccumulator> {

        @Override
        public PvUvAccumulator createAccumulator() {
            return new PvUvAccumulator();
        }

        @Override
        public PvUvAccumulator add(PvUvAccumulator partialResult, PvUvAccumulator globalAccumulator) {
            globalAccumulator.pvCount += partialResult.pvCount;
            globalAccumulator.userIds.addAll(partialResult.userIds); // 【關鍵】合併 Set 實現跨分區全局去重
            return globalAccumulator;
        }

        @Override
        public PvUvAccumulator getResult(PvUvAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public PvUvAccumulator merge(PvUvAccumulator a, PvUvAccumulator b) {
            a.pvCount += b.pvCount;
            a.userIds.addAll(b.userIds);
            return a;
        }
    }

    // ProcessWindowFunction: 格式化輸出 PV 總數和 UV 總數
    // 此函數現在用於第二階段，處理最終的全局 PvUvAccumulator。
    // 輸入: PvUvAccumulator | 輸出: PvUvCount | Key: String
    public static class PvUvAnalysisProcessWindowFunction extends ProcessWindowFunction<PvUvAccumulator, PvUvCount, String, TimeWindow> {

        @Override
        public void process(String key, ProcessWindowFunction<PvUvAccumulator, PvUvCount, String, TimeWindow>.Context context, Iterable<PvUvAccumulator> elements, Collector<PvUvCount> out) throws Exception {
            // AggregateFunction 只會輸出一個結果到 elements 中
            PvUvAccumulator result = elements.iterator().next();

            PvUvCount pvUvCount = new PvUvCount();
            pvUvCount.setPvCount(result.pvCount);
            pvUvCount.setUvCount((long) result.userIds.size()); // UV 總數為最終合併後 Set 的大小
            pvUvCount.setWindowEnd(context.window().getEnd());

            out.collect(pvUvCount);
        }

    }
}
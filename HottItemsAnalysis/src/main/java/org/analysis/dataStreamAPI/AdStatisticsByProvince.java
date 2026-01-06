package org.analysis.dataStreamAPI;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.conf.AnalysisConfig;
import org.pojo.*;
import org.utils.FlinkSource;
import java.util.LinkedHashMap;


import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;

import static org.conf.ConfigLoader.loadAppConfig;

// 統計 每小時  按照省份劃分 每個廣告點擊次數
// 額外 同個用戶多次刷 發警報(測輸出) 後主流不統計
public class AdStatisticsByProvince {


    public static void main(String[] args) throws Exception {

        AnalysisConfig config = loadAppConfig();

        int parallelism = config.adStatisticsByProvinceAnalysis.flink.parallelism;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parallelism);

        String testData = config.adStatisticsByProvinceAnalysis.testData;
        DataStream<String> fileSource = FlinkSource.getFileSource(env, testData);

        // 轉換成物件
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

        // 設定 事件時間欄位 Timestamp + Watermark (使用新的 API)
        DataStream<AdClickLog> withWatermark =
                parsed.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<AdClickLog>forBoundedOutOfOrderness(Duration.ofSeconds(2)) // 允許亂序 2 秒
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()) // 事件時間欄位
                );


        // 遲到資料的 OutputTag
        final OutputTag<AdClickLog> LATE_TAG = new OutputTag<AdClickLog>("Late-AdStatisticsByProvince"){};

        //側流
        final OutputTag<AdClickLogAlarm> Alarm_TAG = new OutputTag<AdClickLogAlarm>("Alarm-AdStatisticsByProvince"){};

        //額外 同個用戶多次刷 發警報(測輸出) 後主流不統計 * 需要清裡內存時機
        SingleOutputStreamOperator<AdClickLog> process = withWatermark.keyBy(new KeySelector<AdClickLog, Tuple2<Long, Long>>() {
                    @Override
                    //這裡使用 Tuple2<Long, Long> 建立一個「複合鍵」(Composite Key)。
                    //有當用戶 ID 和廣告 ID 都相同時，數據才會被分到同一個 ProcessFunction 實例進行處理。
                    public Tuple2<Long, Long> getKey(AdClickLog log) throws Exception {
                        // 這裡回傳的型別是明確的 Tuple2<Long, Long>
                        return Tuple2.of(log.getUserId(), log.getAdvertiseId());
                    }
                })
                .process(new AlarmKeyedProcessFunction(10L, 1000L * 60 * 60 * 24, Alarm_TAG));

        //開1小時滾動窗口 用省份分組
        SingleOutputStreamOperator<AdStatisticsByProvinceProcess> aggregate = process
                .keyBy(AdClickLog::getProvince)
                //事件滾動窗口
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .sideOutputLateData(LATE_TAG)
                //累加分組計算省
                .aggregate(new AggregateFunction<AdClickLog, AdStatisticsByProvinceAccumulator, AdStatisticsByProvinceAccumulator>() {
                    @Override
                    public AdStatisticsByProvinceAccumulator createAccumulator() {
                        return new AdStatisticsByProvinceAccumulator();
                    }

                    @Override
                    public AdStatisticsByProvinceAccumulator add(AdClickLog value, AdStatisticsByProvinceAccumulator accumulator) {
                        //累加 廣告計數
                        accumulator.getAdCount().merge(value.getAdvertiseId(), 1L, Long::sum);
                        return accumulator;
                    }

                    @Override
                    public AdStatisticsByProvinceAccumulator getResult(AdStatisticsByProvinceAccumulator accumulator) {
                        return accumulator;
                    }

                    @Override
                    public AdStatisticsByProvinceAccumulator merge(AdStatisticsByProvinceAccumulator a, AdStatisticsByProvinceAccumulator b) {
                        // 合併兩個分區的 Map 和總數
                        b.getAdCount().forEach((adId, adCount) -> {
                            a.getAdCount().merge(adId, adCount, Long::sum);
                        });
                        return a;
                    }
                    //處理輸出
                }, new ProcessWindowFunction<AdStatisticsByProvinceAccumulator, AdStatisticsByProvinceProcess, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<AdStatisticsByProvinceAccumulator, AdStatisticsByProvinceProcess, String, TimeWindow>.Context context, Iterable<AdStatisticsByProvinceAccumulator> elements, Collector<AdStatisticsByProvinceProcess> out) throws Exception {


                        AdStatisticsByProvinceAccumulator next = elements.iterator().next();
                        //以降序排序 廣告id點擊數
                        Map<Long, Long> sortedByValue = next.getAdCount().entrySet()
                                .stream()
                                .sorted(Map.Entry.<Long, Long>comparingByValue().reversed()) // 降序：點擊多的在前面
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue,
                                        (e1, e2) -> e1,
                                        LinkedHashMap::new
                                ));

                        AdStatisticsByProvinceProcess ap = new AdStatisticsByProvinceProcess();
                        ap.setProvinceId(key);
                        ap.setAdCount(sortedByValue);
                        ap.setWindowEnd(context.window().getEnd());
                        out.collect(ap);
                    }
                });


        //主流
        aggregate.print("AdStatisticsByProvince-Print");

        //遲到流
        aggregate.getSideOutput(LATE_TAG).print("AdStatisticsByProvince-LATE_TAG");

        //黑名單流
        process.getSideOutput(Alarm_TAG).print("AdStatisticsByProvince-Alarm_TAG");

        env.execute("AdStatisticsByProvince");

    }

    public static class AlarmKeyedProcessFunction extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickLog, AdClickLog> {


        // 限制user最大點擊該廣告次數
        private final Long userCountBoundary;

        //清理時間 毫秒
        private final Long cleatTime;

        //側輸出 警報
        private final OutputTag<AdClickLogAlarm>  Alarm_TAG;

        public AlarmKeyedProcessFunction(Long userCountBoundary, Long cleatTime, OutputTag<AdClickLogAlarm> alarm_TAG) {
            this.userCountBoundary = userCountBoundary;
            this.cleatTime = cleatTime;
            Alarm_TAG = alarm_TAG;
        }

        //紀錄用戶點擊該將告次數
        private transient ValueState<Long> userCountStatus;

        //是否發送過警報
        private transient ValueState<Boolean> isAlarmState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            ValueStateDescriptor<Long> userCount = new ValueStateDescriptor<>("userCount", Long.class,0L); // 初始值 0
            userCountStatus = getRuntimeContext().getState(userCount);

            ValueStateDescriptor<Boolean> isAlarm = new ValueStateDescriptor<>("isAlarm", Boolean.class,false); // 初始值 false
            isAlarmState = getRuntimeContext().getState(isAlarm);

        }

        @Override
        public void processElement(AdClickLog value, KeyedProcessFunction<Tuple2<Long, Long>, AdClickLog, AdClickLog>.Context ctx, Collector<AdClickLog> out) throws Exception {

            Long currentCount = userCountStatus.value();

            // 判斷是否是該 Key 的第一筆數據進來
            if(currentCount == 0){
                // 註冊 Processing Time Timer，用於定時清理狀態
                TimerService timerService = ctx.timerService();
                timerService.registerProcessingTimeTimer(
                        timerService.currentProcessingTime() + cleatTime
                );
            }

            //  判斷點擊後是否會超過最大限制 (重點優化區域)
            if (currentCount + 1 > userCountBoundary) {

                // 點擊次數已達到或超過限制 (例如：限制 10 次，這是第 11 次)

                // 檢查是否是第一次超過限制
                if (!isAlarmState.value()) {
                    // 第一次超過，發送警報，並將狀態設為 true
                    ctx.output(Alarm_TAG, new AdClickLogAlarm(value.getUserId(), value.getAdvertiseId(), value.getTimestamp()));
                    isAlarmState.update(true);
                }

                // 無論是否第一次發送警報，所有超過限制的點擊都應被過濾掉 (不輸出到主流)
                return;
            }

            // 未超過限制：更新計數並輸出到主流
            userCountStatus.update(currentCount + 1);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple2<Long, Long>, AdClickLog, AdClickLog>.OnTimerContext ctx, Collector<AdClickLog> out) throws Exception {
            //清理緩存
            userCountStatus.clear();
            isAlarmState.clear();
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }


}

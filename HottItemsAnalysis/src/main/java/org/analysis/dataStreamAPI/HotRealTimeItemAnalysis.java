package org.analysis.dataStreamAPI;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.pojo.Enums.UserBehaviorEnum;
import org.pojo.ItemViewCount;
import org.pojo.UserBehavior;
import org.conf.AnalysisConfig;
import org.utils.FlinkSource;
import org.utils.KafkaTopicUtil;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.conf.ConfigLoader.loadAppConfig;

//熱門實時商品統計

//需求:
//統計近1小時內的熱們商品 每五分鐘更新一次
//熱門度用瀏覽次數 ("pv") 來衡量
//由count高到低排序
//輸出: 商品:count

//思路:
//在所有用戶行為數據中 過濾 pv 行為統計
//構建滑動窗口 窗口長度1小時 滑動距離5分鐘
//用事件窗口結束時間設定時器來確保所有數據都到齊後排序

//展示: Aggregate累加, SlidingEventTimeWindows滑動窗口, OnTimer定時器

//數據格式:543462,1715,1464116,pv,1511658000
//送訊息
//docker exec -it kafka kafka-console-producer \
//        --broker-list localhost:9092 \
//        --topic HotRealTimeItemAnalysis_topic

public class HotRealTimeItemAnalysis {

    public static void main(String[] args) throws Exception {

        //窗口的延遲關閉時間 延遲衝口來等待數據 可以搭配定時器來控制統計數據的緩存

        AnalysisConfig config = loadAppConfig();

        Long allowedLateness = config.hotRealTimeItemAnalysis.flink.allowedLateness;
        String topic = config.hotRealTimeItemAnalysis.kafka.topic;
        String group = config.hotRealTimeItemAnalysis.kafka.group;
        String host = config.hotRealTimeItemAnalysis.kafka.host;
        String port = config.hotRealTimeItemAnalysis.kafka.port;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        String path = config.hotRealTimeItemAnalysis.testData;
//        FileInputFormat<String> input = new AnalysisFileInputFormat(new Path(path));
//        // 讀檔
//        DataStream<String> source = env.readFile(input, path);

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
                userBehavior.setBehavior(UserBehaviorEnum.getByValue(split[3]));
                //**目前時間戳是 1511658000 必須*1000 變為毫秒
                userBehavior.setTimestamp(Long.parseLong(split[4]) * 1000L);
                return userBehavior;
            }
        });

        // 遲到資料的 OutputTag
        final OutputTag<UserBehavior> LATE_TAG = new OutputTag<UserBehavior>("Late-HotRealTimeItemAnalysis"){};

        // 先設定 事件時間欄位 Timestamp + Watermark
        DataStream<UserBehavior> withWatermark =
                parsed.assignTimestampsAndWatermarks(

                        //**判斷升序還是亂序 目前測試資料是有序 升序
                        //** 降序不適合流式大數據分析 給Flink之前最好先生序排序
                        //** 亂序太嚴重要確保數據正確必須延長時間 會降低速度

                        // 升序數據設置事件時間
                        new  AscendingTimestampExtractor<UserBehavior>() {
                            @Override
                            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                                return userBehavior.getTimestamp(); // 事件時間欄位
                            }
                        }

                        //亂序數據設置事件時間 觸發計算
//                        new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(
//                                Duration.ofSeconds(2) //允許亂序 2 秒
//                        ) {
//                            @Override
//                            public long extractTimestamp(UserBehavior m) {
//                                return m.getTimestamp(); // 事件時間欄位
//                            }
//                        }
                );

        //過濾出用戶行為
        SingleOutputStreamOperator<UserBehavior> pvStream = withWatermark.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return value.getBehavior().equals(UserBehaviorEnum.PV);
            }
        });

        //keyBy 用商品id來分
        KeyedStream<UserBehavior, Long> keyedStream = pvStream.keyBy(new KeySelector<UserBehavior, Long>() {
            @Override
            public Long getKey(UserBehavior value) throws Exception {
                return value.getItemId();
            }
        });

        SingleOutputStreamOperator<ItemViewCount> aggregate = keyedStream
                //事件滑動窗口 窗口大小 1 小時，每 5 五分鍾滑動一次 會重疊
                .window(SlidingEventTimeWindows.of(Duration.ofHours(1), Duration.ofMinutes(5)))
                .allowedLateness(Duration.ofSeconds(allowedLateness)) //允許延遲 30 秒
                .sideOutputLateData(LATE_TAG) // 處理延數據 超過 watermark + allowedLateness 的才會進來
                //AggregateFunction+ProcessWindowFunction 窗口會在AggregateFunction , ProcessWindowFunction 會等窗口時間到才執行
                .aggregate(
                        //AggregateFunction<待聚合值（輸入值）的類型, 累加器（中間聚合狀態）的型別, 聚合結果的類型>
                        new AggregateFunction<UserBehavior, Long, Long>() {

                            @Override
                            public Long createAccumulator() {
                                return 0L; // count 初始值
                            }

                            @Override
                            public Long add(UserBehavior value, Long accumulator) {
                                return accumulator + 1;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator; // 傳遞到 ProcessWindowFunction
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return a + b; // 合併窗口
                            }
                        },

                        //處理成 ItemViewCount
                        // ProcessWindowFunction<輸入值的類型, 輸出值的類型, Key的類型, 此視窗函數可套用的視窗類型>
                        new ProcessWindowFunction<Long, ItemViewCount, Long, TimeWindow>() {

//                            process(用於AggregateFunction的Key, 視窗中的上下文,AggregateFunction的視窗中的元素, 用於發出元素的收集器)
                            @Override
                            public void process(Long itemId, Context context, Iterable<Long> counts, Collector<ItemViewCount> out) {

                                // 取出窗口聚合後的瀏覽次數（因為使用 AggregateFunction，所以只會有單一值）
                                Long count = counts.iterator().next();

                                // 取得這個窗口的結束時間（毫秒）
                                Long windowEnd = context.window().getEnd();

                                // 將 itemId、窗口結束時間、count 包裝成結果物件並輸出
                                out.collect(new ItemViewCount(itemId, windowEnd, count));
                            }
                        }
                );


        //打印數據
//        aggregate.print("HotRealTimeItemAnalysis");

        // --- 新增 Top N 排序步驟 ---

        // 1. 根據窗口結束時間 (windowEnd) 進行 KeyBy，確保同一窗口結束時間的所有 ItemViewCount 都會進入同一個 KeyedProcessFunction 實例
        SingleOutputStreamOperator<String> ranking = aggregate
                .keyBy(new KeySelector<ItemViewCount, Long>() {
                    @Override
                    public Long getKey(ItemViewCount value) throws Exception {
                        return value.getWindowEnd(); // 以窗口結束時間作為 Key
                    }
                })
                // 2. 應用 TopNHotItems 函式進行排序和輸出
                .process(new TopNHotItems(5,100,allowedLateness)); // 這裡設定 Top 5 KeyedProcessFunction<Key型別,輸入元素的類型,輸出元素的型別> 這裡的輸入是 上個流得輸出


        // 打印數據 (改為打印 ranking 數據)
        ranking.print("HotRealTimeItemTopN");


        // 取出遲到資料 可以依照業務做補救  例:多個最終結果流 正常結果流 + 遲到流 → union → 當作最後的結果來源
        aggregate.getSideOutput(LATE_TAG).print("LATE DATA");

        env.execute();

    }



    // Top N 排序函數
    // KeyedProcessFunction<Key型別,輸入元素的類型,輸出元素的型別> 這裡的輸入是 上個流得輸出
    public static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {

        //熱門n
        private final int topSize;

        //窗口延遲時間 可以用來等待晚點來的數據一起統計
        private final int windowDelayTime;

        private final Long allowedLateness;

        public TopNHotItems(int topSize,int windowDelayTime, Long allowedLateness) {
            this.topSize = topSize;
            this.windowDelayTime = windowDelayTime;
            this.allowedLateness = allowedLateness;
        }

        // 狀態：用於儲存每個窗口結束時間（Key）下的所有 ItemViewCount 列表
        // ListState<ItemViewCount> allItems;
        // 為了更方便地按 count 排序，使用 MapState<itemId, count> 更合適，但這裡為了展示數據完整性，用 ListState
        private transient ListState<ItemViewCount> itemState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            // 初始化狀態 定義描述器
            itemState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>(
                            "item-count-list",
                            ItemViewCount.class
                    )
            );
        }

        // 處理每個 ItemViewCount 數據
        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 將當前商品及其計數新增到狀態中
            itemState.add(value);

            // 註冊一個事件時間定時器，用於在窗口結束時間（Key）到達時延後 n 毫秒觸發
            //* 如果相同 Key 在相同時間戳上已經註冊過，Flink 不會重複註冊，只會保留一個。
            //* 這裡因為使用窗口時間來開定時器 所以不怕重複註冊
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+ windowDelayTime);

            //註冊一個窗口關閉時間 依照.allowedLateness(Duration.ofSeconds(30)) //允許延遲 30 秒 如果窗口真正被關閉會到測輸出流
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+ allowedLateness);
        }


        // 定時器觸發，即一個窗口結束時觸發
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //先判斷是否到了清空窗口時間 如果是直接清翁狀態返回 根據的時間是 .allowedLateness(Duration.ofSeconds(allowedLateness)) //允許延遲 30 秒 等待後窗吼會卻關閉
            //如果沒有衝口還存在就計算 包含allowedLateness(Duration.ofSeconds(30)) 設定的等待時間都會在輸出一次
            //目前得 getCurrentKey 是 WindowEnd
            if (timestamp == ctx.getCurrentKey() + allowedLateness*1000) {
                // 4. 清空狀態，釋放內存
                itemState.clear();
            }

            // 1. 從狀態中取出所有 ItemViewCount
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }

            // 2. 進行排序：按 count 從高到低排序
            allItems.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));

            // 3. 格式化輸出 Top N 結果
            StringBuilder result = new StringBuilder();
            result.append("========================================\n");
            result.append("窗口結束時間: ").append(timestamp).append(" (").append(new java.util.Date(timestamp - windowDelayTime)).append(")\n");
            result.append("熱門商品 Top ").append(topSize).append(":\n");

            for (int i = 0; i < Math.min(topSize, allItems.size()); i++) {
                ItemViewCount currentItem = allItems.get(i);
                result.append("  No.").append(i + 1).append(":")
                        .append(" 商品ID=").append(currentItem.getItemId())
                        .append(" 瀏覽量=").append(currentItem.getCount())
                        .append("\n");
            }
            result.append("========================================\n");


//            Thread.sleep(1000);

            out.collect(result.toString());

            // 4. 清空狀態，釋放內存
//            itemState.clear();
        }

        @Override
        public void close() throws Exception {
            //這裡清理的話會把 KeyedProcessFunction 的緩存清掉 但是窗口還沒關 所以會讓 allowedLateness 來的數據統計時錯誤 用定時器來另外處理
//            itemState.clear();
            super.close();
        }
    }
}

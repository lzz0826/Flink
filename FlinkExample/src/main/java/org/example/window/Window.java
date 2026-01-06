package org.example.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.MyObservation;

import java.time.Duration;

//  開窗函數
public class Window {
    public static void main(String[] args) throws Exception {

        //建立 Flink 環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //測試 Socket :
        //socat -v TCP-LISTEN:9999,reuseaddr,fork -
        //測試數據:0001,equipment1,34.1,1762392490
        DataStream<String> source = env.socketTextStream("localhost", 9999);

        //轉換成JAVA物件
        DataStream<MyObservation> myDataStream = source.map(new MapFunction<String, MyObservation>() {
            @Override
            public MyObservation map(String in) throws Exception {
                String[] split = in.split(",");
                MyObservation myObservation = new MyObservation();
                myObservation.setId(Integer.valueOf(split[0]));
                myObservation.setName(split[1]);
                myObservation.setTemperature(Double.parseDouble(split[2]));
                myObservation.setTime(Long.parseLong(split[3]));
                return myObservation;
            }
        });

//        | 特性          | 增量函數 (Reduce / Aggregate)                                          | 全窗口函數 (Process / Apply)                                                     |
//        | -------      | -------------------------------------------------------------------   | --------------------------------------------------------------------------- |
//        | 計算方式      | 每來一條數據就更新累加器（Incremental）                                    | 等窗口結束，才拿到整個窗口內的所有元素                                                         |
//        | 記憶體需求    | 低，只需維護累加器                                                        | 高，要存整個窗口內的所有元素                                                              |
//        | 輸入與輸出類型 | Reduce：輸入 = 輸出 <br> Aggregate：輸入可不同於輸出                       | 輸入 = 整個窗口元素集合，輸出可任意類型                                                       |
//        | 適合場景      | 計算總和、最大值、最小值、平均（可用累加器）                                  | 複雜操作：排序、Top N、列表、窗口時間控制                                                     |
//        | 例子          | `reduce((v1,v2)->v1+v2)` <br> `aggregate(new AggregateFunction...)`   | `process(new ProcessWindowFunction...)` <br> `apply(new WindowFunction...)` |


        // -------------------------  增量函數  ---------------------------

//                | 特性               | ReduceFunction               | AggregateFunction
//                | -------            | ---------------              | ---------------------
//                | 輸入/輸出類型        | 相同                          | 可以不同（IN → ACC → OUT）
//                | 是否需要累加器       | 不需要額外類型，直接用元素本身     | 需要累加器，可是不同類型
//                | 適合場景            | 最大值、最小值、簡單累加          | 更複雜的聚合、類型轉換、分佈式 merge
//                | API 複雜度          | 簡單                          | 較複雜，但更靈活
//                | 增量計算            | 可                            | 可

//        tumblingWindow(myDataStream);// 時間滾動窗口 (Tumbling Window) *滾動式計算
//        slidingWindow(myDataStream);// 時間滑動窗口 (Sliding Window)
//        sessionWindow(myDataStream);// 會話窗口 (Session Window)
//        tumblingCountWindow(myDataStream);// 滾動計數窗口 (Tumbling Count Window) *滾動式計算
//        sessionCountWindow(myDataStream);// 滑動計數窗口 (Sliding Count Window)

//        aggregate(myDataStream);//時間滾動窗口 aggregate

        // -------------------------  全窗口函數  ---------------------------

//                | 特性          | apply    | process                   |
//                | -------      | -----     | -----------------------  |
//                | 拿整個窗口資料 | 可        | 可                        |
//                | 可拿窗口元數據 | 不        | 可（start/end, watermark） |
//                | API 複雜度    | 簡單      | 較複雜                     |
//                | 靈活性        | 中等      | 高                        |

//        apply(myDataStream);//時間窗口 全窗口函數
//        process(myDataStream);//時間窗口 全窗口函數


        // ------------------------- 其他可選API -------------------------
        //        .trigger()//觸發器
//                .evictor()//移除器
//                .allowedLateness(Duration.ofSeconds(1))//允許處理遲到的數據
//                .sideOutputLateData();//將遲到的數據放到測輸出流

//        slidingWindowWithFullConfigs(myDataStream);



        env.execute();

    }

    /** ----------------------------------------------------
     *   滾動窗口 (Tumbling Window) *滾動式計算
     *   每 10 秒計算一次，不重疊
     * ---------------------------------------------------- */
    private static void tumblingWindow(DataStream<MyObservation> myDataStream ) throws Exception {
        // keyBy
        KeyedStream<MyObservation, Integer> keyedStream = myDataStream.keyBy(MyObservation::getId);
        keyedStream
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .reduce((v1, v2) -> v1)
                .name("Tumbling Window")
                .print();
    }

    /** ----------------------------------------------------
     *   滑動窗口 (Sliding Window)
     *   窗口大小 10 秒，每 5 秒滑動一次
     *   會重疊
     * ---------------------------------------------------- */
    private static void slidingWindow(DataStream<MyObservation> myDataStream ){
        // keyBy
        KeyedStream<MyObservation, Integer> keyedStream = myDataStream.keyBy(MyObservation::getId);
        keyedStream
                .window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .reduce((v1, v2) -> v1)
                .name("Sliding Window")
                .print();
    }

    /** ----------------------------------------------------
     *   會話窗口 (Session Window)
     *   若 5 秒都沒收到同一 key 的資料 → 視為 session 結束
     * ---------------------------------------------------- */
    private static void sessionWindow(DataStream<MyObservation> myDataStream){
        // keyBy
        KeyedStream<MyObservation, Integer> keyedStream = myDataStream.keyBy(MyObservation::getId);
        keyedStream
                .window(ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(5)))
                .reduce((v1, v2) -> v1)
                .name("Session Window")
                .print();
    }

    /** ----------------------------------------------------
     *   滾動計數窗口 (Tumbling Count Window) *滾動式計算
     *   每收到 3 筆相同 key 的事件 → 輸出一次
     * ---------------------------------------------------- */
    private static void tumblingCountWindow(DataStream<MyObservation> myDataStream){
        // keyBy
        KeyedStream<MyObservation, Integer> keyedStream = myDataStream.keyBy(MyObservation::getId);
        keyedStream
                .countWindow(3)
                .reduce((v1, v2) -> v1)
                .name("Tumbling Count Window")
                .print();
    }

    /** ----------------------------------------------------
     *   滑動計數窗口 (Sliding Count Window)
     *   窗口大小 5 筆，每滑動 2 筆 → 計算一次
     *   會重疊
     * ---------------------------------------------------- */
    private static void sessionCountWindow(DataStream<MyObservation> myDataStream){
        // keyBy
        KeyedStream<MyObservation, Integer> keyedStream = myDataStream.keyBy(MyObservation::getId);
        keyedStream
                .countWindow(5, 2)
                .reduce((v1, v2) -> v1)
                .name("Sliding Count Window")
                .print();
    }

    /** ----------------------------------------------------
     *   滾動窗口 (Tumbling Window) *滾動式計算
     *   每 10 秒計算一次，不重疊 計算每個id , 10秒出現次數
     *   aggregate 增量函數
     * ---------------------------------------------------- */
    private static void aggregate(DataStream<MyObservation> myDataStream){
        // keyBy
        KeyedStream<MyObservation, Integer> keyedStream = myDataStream.keyBy(MyObservation::getId);
        keyedStream
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                //AggregateFunction<待聚合值（輸入值）的類型, 累加器（中間聚合狀態）的型別, 聚合結果的類型>
                .aggregate(new AggregateFunction<MyObservation, AccumulatorClass, String>() {

                    // 創建累加器（Accumulator）的初始值
                    // 每個窗口開始時都會呼叫此方法來初始化累加器
                    @Override
                    public AccumulatorClass createAccumulator() {
                        return new AccumulatorClass(0,0);
                    }

                    // 累加邏輯 每當窗口內有新元素到達時呼叫
                    // value: 當前元素
                    // accumulator: 目前累加的結果
                    // 返回新的累加值
                    @Override
                    public AccumulatorClass add(MyObservation value, AccumulatorClass accumulator) {
                        //這裡接收到的 value 已經是根據 id 分過組的
                        accumulator.setId(value.getId());
                        accumulator.setCount(accumulator.getCount() + 1);
                        return accumulator;
                    }

                    // 從累加器獲取結果
                    // 窗口計算結束時，Flink 會呼叫此方法來取得最終結果
                    // 返回累加器的值
                    @Override
                    public String getResult(AccumulatorClass accumulator) {
                        return accumulator.toString();
                    }

                    // 合併累加器（在分佈式場景下使用）
                    @Override
                    public AccumulatorClass merge(AccumulatorClass a, AccumulatorClass b) {
                        // 當窗口跨分區時，Flink 會把多個累加器合併
                        // a、b 分別是兩個累加器的值，返回合併後的值
                        return new AccumulatorClass(a.id, a.getCount() + b.getCount());
                    }
                })
                .name("aggregate")
                .print();
    }


    @Data
    @AllArgsConstructor
    private static class AccumulatorClass{
        Integer id;
        Integer count;
    }


    /** ----------------------------------------------------
     *   滾動窗口 (Tumbling Window) *滾動式計算
     *   每 10 秒計算一次，不重疊 計算每個id
     *   全窗口函數 apply
     * ---------------------------------------------------- */
    private static void apply(DataStream<MyObservation> myDataStream) {
        // keyBy
        KeyedStream<MyObservation, Integer> keyedStream = myDataStream.keyBy(MyObservation::getId);
        keyedStream
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                //WindowFunction<輸入值的類型, 輸出值的類型, Key的類型, 此視窗函數可套用的視窗類型>
                .apply(new WindowFunction<MyObservation, String, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer key, TimeWindow window, Iterable<MyObservation> input, Collector<String> out) throws Exception {
                        int count = 0;
                        double sumTemp = 0;
                        for (MyObservation obs : input) {
                            sumTemp += obs.getTemperature();
                            count++;
                        }
                        double avg = count == 0 ? 0 : sumTemp / count;
                        out.collect("Key: " + key + ", AvgTemp: " + avg);
                    }
                }).print();
    }


    /** ----------------------------------------------------
     *   滾動窗口 (Tumbling Window) *滾動式計算
     *   每 10 秒計算一次，不重疊 計算每個id
     *   全窗口函數 process
     * ---------------------------------------------------- */
    private static void process(DataStream<MyObservation> myDataStream){
        // keyBy
        KeyedStream<MyObservation, Integer> keyedStream = myDataStream.keyBy(MyObservation::getId);
        keyedStream
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                //ProcessWindowFunction<輸入值的類型,輸出值的類型,按鍵的類型,此視窗函數可套用的視窗類型>
                .process(new ProcessWindowFunction<MyObservation, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer key, Context context, Iterable<MyObservation> elements, Collector<String> out) throws Exception {
                        int count = 0;
                        double sumTemp = 0;
                        for (MyObservation obs : elements) {
                            sumTemp += obs.getTemperature();
                            count++;
                        }
                        double avg = count == 0 ? 0 : sumTemp / count;
                        out.collect("Key: " + key + ", Window ["
                                + context.window().getStart() + ","
                                + context.window().getEnd() + "], AvgTemp: " + avg);
                    }
                }).print();
    }

    // 用來接收遲到資料的 OutputTag
    private static final OutputTag<MyObservation> LATE_TAG =
            new OutputTag<MyObservation>("late-data"){};

    private static void slidingWindowWithFullConfigs(DataStream<MyObservation> myDataStream) {

        KeyedStream<MyObservation, Integer> keyedStream =
                myDataStream.keyBy(MyObservation::getId);

        SingleOutputStreamOperator<MyObservation> result =
                keyedStream
                        .window(SlidingProcessingTimeWindows.of(
                                Duration.ofSeconds(10),  // 視窗大小
                                Duration.ofSeconds(5)))  // 滑動間隔
                        // 自訂 Trigger（觸發器）
                        .trigger(new MyTrigger())

                        // 自訂 Evictor（移除器）
                        .evictor(new MyEvictor() {
                        })

                        // 允許 1 秒遲到資料
                        .allowedLateness(Duration.ofSeconds(1))

                        // 遲到資料輸出到 Side Output
                        .sideOutputLateData(LATE_TAG)

                        // 使用 Reduce 作為增量聚合
                        .reduce((v1, v2) -> v1); // 只是示範：用第一筆資料

        // 正常窗口輸出
        result.print("WINDOW RESULT");

        // 遲到資料輸出
        result.getSideOutput(LATE_TAG).print("LATE DATA");
    }

}



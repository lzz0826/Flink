package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//KeyedProcessFunction 更通用的處理
public class ProcessFunctionExample {

    public static void main(String[] args) throws Exception {

        // 建立 Flink 環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 測試 Socket : socat -v TCP-LISTEN:9999,reuseaddr,fork -
        // 測試數據範例: 0001,equipment1,29.1,1762392490
        DataStream<String> source = env.socketTextStream("localhost", 9999);

        // 轉換成 Java 物件
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

        // KeyedProcessFunction<鍵的型別, 輸入元素的類型, 輸出元素的型別>
//        myDataStream.keyBy(MyObservation::getId).process(new MyKeyedProcessFunction() {
//        }).print();

        //需求 10秒內同個設備 溫度連續上升 發警報
        //KeyedProcessFunction<鍵的型別, 輸入元素的類型, 輸出元素的型別>
        myDataStream.keyBy(MyObservation::getId).process(new TemperatureProcessFunction(10) {
        }).print();


        //低溫測流
        OutputTag<MyObservation> outputTag = new OutputTag<>("low") {
        };
        //需求 分高溫流high主流 低溫流low測流
        SingleOutputStreamOperator<MyObservation> high = myDataStream.process(new HightLowProcessFunction(30 ,outputTag) {
        });
        //輸出高溫流 主流
        high.print("high");
        //輸出低溫流 測流
        high.getSideOutput(outputTag).print("low");

        env.execute();

    }

    public static class MyKeyedProcessFunction extends KeyedProcessFunction<Integer, MyObservation, String> {

        // 保存每個 key 的時間戳狀態（ValueState）
        private transient ValueState<Long> tsTimeStatus;

        @Override
        public void open(OpenContext openContext) throws Exception {
            // 定義 ValueState 的描述器，用來儲存 Long 型別的時間戳
            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>("tsTimeStatus", Long.class); // 初始值 0
            tsTimeStatus = getRuntimeContext().getState(descriptor);

        }

        //processElement 有資料進來就立即執行一次
        @Override
        public void processElement(MyObservation value, KeyedProcessFunction<Integer, MyObservation, String>.Context ctx, Collector<String> out) throws Exception {

            // 輸出數據
            out.collect(value.getId() + "," + value.getName() + "," + value.getTemperature() + "," + value.getTime());

            // 取得當前元素的事件時間（可能為 null，取決於來源是否有 watermark）
            ctx.timestamp();

            // 取得當前 key (由 keyBy 決定)
            ctx.getCurrentKey();

//            ctx.output(); // 側輸出

            // TimerService 可以註冊事件時間或處理時間定時器
            TimerService timerService = ctx.timerService();

            // 取得目前的 watermark（事件時間）
            timerService.currentWatermark();

            // 取得目前的處理時間（系統時間）
            timerService.currentProcessingTime();//當前時間搓

            // 更新 ValueState（儲存目前這筆資料的時間）
            tsTimeStatus.update(value.getTime());

            // 註冊 1 秒後觸發的處理時間定時器 依照「系統時間」觸發
            timerService.registerProcessingTimeTimer(
                    ctx.timerService().currentProcessingTime() + 1000L
            );

            // 註冊事件時間定時器（10 秒後）依照「資料的事件時間」觸發
            timerService.registerEventTimeTimer(
                    value.getTime() + 10 * 1000L
            );

//            timerService.deleteProcessingTimeTimer(tsTimeStatus.value()); // 刪除指定的 已註冊定時器

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Integer, MyObservation, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

            // 當定時器觸發時會執行這裡
            System.out.println(timestamp + " 定時器觸發 ");

            // 取得當前 key
            ctx.getCurrentKey();

//            ctx.output(); // 側輸出（註解掉）

            // 判斷是事件時間 or 處理時間
            ctx.timeDomain();

        }

        @Override
        public void close() throws Exception {
            super.close();
            // 程式結束時清除 state
            tsTimeStatus.clear();
        }
    }


    //需求 10秒內同個設備 溫度連續上升 發警報
    public static class TemperatureProcessFunction extends KeyedProcessFunction<Integer, MyObservation, String> {

        private final Integer second;

        public TemperatureProcessFunction(Integer second) {
            this.second = second;
        }

        //保存上個溫度
        private transient ValueState<Double> lastTemperatureStatus;

        //紀錄上升次數 如果下降的話歸零
        private transient ValueState<Integer> countTemperatureStatus;

        // 保存已註冊的定時器時間，避免重複註冊
        private transient ValueState<Long> nextTimerStatus;

        @Override
        public void open(OpenContext openContext) throws Exception {

            ValueStateDescriptor<Double> descriptor =
                    new ValueStateDescriptor<>("lastTemperatureStatus", Double.class);
            lastTemperatureStatus = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor<Integer> countTemperatureDescriptor =
                    new ValueStateDescriptor<>("countTemperatureStatus", Integer.class,0); // 初始值 0
            countTemperatureStatus = getRuntimeContext().getState(countTemperatureDescriptor);

            nextTimerStatus = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("nextTimerStatus", Long.class)
            );
        }

        //processElement 有資料進來就立即執行一次
        @Override
        public void processElement(MyObservation value, KeyedProcessFunction<Integer, MyObservation, String>.Context ctx, Collector<String> out) throws Exception {

            System.out.println("processElement:"+value.getId() + "," + value.getName() + "," + value.getTemperature() + "," + value.getTime());

            Double last = lastTemperatureStatus.value();
            double temperature = value.getTemperature();
            Long currentTimer = nextTimerStatus.value();

            if (last != null) {
                // 溫度上升
                if (temperature > last) {

                    // 第一次發現上升 & 尚未有 timer -> 建立 timer
                    if (currentTimer == null) {
                        countTemperatureStatus.update(countTemperatureStatus.value() + 1);//更新上升計數
                        long timerTs = ctx.timerService().currentProcessingTime() + second * 1000L;
                        ctx.timerService().registerProcessingTimeTimer(timerTs);//註冊定時器
                        nextTimerStatus.update(timerTs);//保存定時器標記
                    }else {
                        // 若已經有 timer，（timer 繼續跑）
                        countTemperatureStatus.update(countTemperatureStatus.value() + 1);//更新上升計數
                    }

                    // 溫度下降
                } else if (temperature < last) {

                    // 有 timer 的話就把它刪掉，表示這段「連續上升」結束了
                    if (currentTimer != null) {
                        //刪除指定的 已註冊定時器
                        ctx.timerService().deleteProcessingTimeTimer(currentTimer);
                        nextTimerStatus.clear();//清理定時器保持null
                    }

                    // 上升計數歸零
                    countTemperatureStatus.update(0);

                    // 下降時不會再幫這筆資料重設 timer
                }
                // 若 temperature == last：既不是上升也不是下降，這邊選擇「不動 timer」，視為不中斷那一段
                // 如果你要把相等也視為中斷，再加一個 else 分支去刪 timer 即可
            }

            // 更新上次溫度
            lastTemperatureStatus.update(temperature);

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Integer, MyObservation, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

            System.out.println(timestamp + " 定時器觸發: id:" + ctx.getCurrentKey()+"累計上升次數:"+countTemperatureStatus.value());

            // 這顆 timer 觸發時，代表這段期間內「沒有下降把它刪掉」
            // -> 表示這 10 秒期間溫度一直處於「非下降且有上升過」的狀態
            if (countTemperatureStatus.value() >= 1) {
                out.collect("警報: id=" + ctx.getCurrentKey());
            }

            // 一輪結束，把狀態重置，讓之後可以重新開始一段新的「連續上升」檢查
//            countTemperatureStatus.update(0);//看要不要累計
            nextTimerStatus.clear(); //清理定時器保持null

        }

        @Override
        public void close() throws Exception {
            // 程式結束時清除 state
            if (lastTemperatureStatus != null) {
                lastTemperatureStatus.clear();
            }
            if (countTemperatureStatus != null) {
                countTemperatureStatus.clear();
            }
            if (nextTimerStatus != null) {
                nextTimerStatus.clear();
            }
        }
    }


    //需求 分高溫流high主流 低溫流low測流
    public static class HightLowProcessFunction extends ProcessFunction<MyObservation,MyObservation>{

        private Integer demarcation;

        //測數據流 低溫流low
        private final OutputTag<MyObservation> outputTag;

        public HightLowProcessFunction(Integer demarcation,OutputTag<MyObservation> outputTag) {
            this.demarcation = demarcation;
            this.outputTag = outputTag;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
        }

        //每條數據進來處理
        @Override
        public void processElement(MyObservation value, ProcessFunction<MyObservation, MyObservation>.Context ctx, Collector<MyObservation> out) throws Exception {
            double temperature = value.getTemperature();
            //高溫流輸出 主流
            if (temperature >= demarcation) {
                out.collect(value);
            }else {
                //低文流輸出 測流
                ctx.output(outputTag, value);
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }


}

package org.example.status;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.MyObservation;

public class KeyedStatus {

    public static void main(String[] args) throws Exception {

        // 建立 Flink 環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 測試 Socket : socat -v TCP-LISTEN:9999,reuseaddr,fork -
        // 測試數據範例: 0001,equipment1,34.1,1762392490
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

        // 必須先 keyBy 才能使用 Keyed State
        myDataStream
                .keyBy(MyObservation::getId) // 按設備名稱作為 key，每個設備有獨立狀態
                .flatMap(new MyFlatMapFunction(10.0)) // 閾值 10 度
                .print();

        env.execute();
    }

    /**
     * RichFlatMapFunction + ValueState 範例：
     * 監控每個設備的溫度變化，當與上次溫度差超過閾值時發出警報
     */
    public static class MyFlatMapFunction
            extends RichFlatMapFunction<MyObservation, Tuple3<String, Double, Double>> {

        // 溫度閾值
        private final Double thresholdTemperature;

        // 定義 Keyed State，保存每個設備上一次的溫度
        private ValueState<Double> lastTemperatureState;
        // 其他狀態類型聲明
//        private ListState<MyObservation> listStatus;
//        private MapState<String, MyObservation> mapState;


        public MyFlatMapFunction(Double thresholdTemperature) {
            this.thresholdTemperature = thresholdTemperature;
        }


        @Override
        public void open(OpenContext openContext) throws Exception {
            // 初始化 ValueState
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("lastTemperatureState", Double.class);
//            ListStateDescriptor listStatus = new ListStateDescriptor<>("listStatus", MyObservation.class);
//            MapStateDescriptor<String, MyObservation> mapState = new MapStateDescriptor<>("mapState",String.class,MyObservation.class);
            lastTemperatureState = getRuntimeContext().getState(descriptor);
            super.open(openContext);
        }

        @Override
        public void flatMap(MyObservation value, Collector<Tuple3<String, Double, Double>> out) throws Exception {

            // 取出 Keyed State
            Double lastValue = lastTemperatureState.value();

            // 初次接收到溫度，保存並返回
            if (lastValue == null) {
                lastTemperatureState.update(value.getTemperature());
                return;
            }

            // 判斷上下波動是否超過閾值
            if (Math.abs(lastValue - value.getTemperature()) > thresholdTemperature) {
                // 輸出：設備名, 上次溫度, 當前溫度
                out.collect(new Tuple3<>(value.getName(), lastValue, value.getTemperature()));
            }

            // 更新 Keyed State 為本次溫度
            lastTemperatureState.update(value.getTemperature());
        }

        @Override
        public void close() throws Exception {
            lastTemperatureState.clear();
            super.close();
        }
    }
}

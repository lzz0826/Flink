package org.example;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;
import org.example.MyFlatMapFunction;

//Flink Wew介面:
//打包JAVA後在上傳後配置: org.example.SocketWordCount
//無界數據流處理 --host localhost --port 8888
//需要開看監聽  % socat -v TCP-LISTEN:8888,reuseaddr,fork -

//IDEA上測試:
//專案目錄下的main起動配置:org.example.SocketWordCount
//main上帶入的啟動參數:--host localhost --port 8888
//Run Edit Configuration 添加: Add dependencies with "provided" scope to classpath
//需要開看監聽  % socat -v TCP-LISTEN:8888,reuseaddr,fork -

//Docker起的 Flink Wew介面:
//專案目錄下的main起動配置:org.example.SocketWordCount
//main上帶入的啟動參數 ** 因為docker使用外部宿主機的Sock:--host.docker.internal --port 8888
//需要開看監聽  % socat -v TCP-LISTEN:8888,reuseaddr,fork -

//Socket 數據流
public class SocketWordCount {

    public static void main(String[] args) throws Exception {
        // 創建 數據流環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 設置並行 每個任務都可已設至並行和多線程 Flink每個任務都是一個線程在等待執行上個任務的結果
        env.setParallelism(2);

        // 工具從啟動參數提取配置 main的 args
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // DataStreamSource<String> stringDataStreamSource =
        // env.socketTextStream("localhost", 8888);
        // DataStreamSource<String> stringDataStreamSource =
        // env.socketTextStream("host.docker.internal", 8888);//Docker
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream(host, port);

        // 從socket取數據
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatted = stringDataStreamSource
                .flatMap(new MyFlatMapFunction()).setParallelism(1);

        // key第一個參數 sum第二個參數
        // flatted.keyBy(0).sum(1).print();

        // (word, 1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = flatted
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;// 取第一個當key
                    }
                }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
                            Tuple2<String, Integer> value2) {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                }).setParallelism(2);
        sum.print();
        // 流需要執行任務
        env.execute();

    }

}

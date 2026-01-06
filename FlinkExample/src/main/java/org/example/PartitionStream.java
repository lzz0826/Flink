package org.example;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.core.fs.Path;
import org.example.utils.MyFileInputFormat;


//分區流分配
public class PartitionStream {
    public static void main(String[] args) throws Exception {
        String path = "/Users/sai/IdeaProjects/Flink/inputData/data2.txt";

        // 建立 Flink 流式執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        MyFileInputFormat stringFileInputFormat = new MyFileInputFormat(new Path(path));

        // 從檔案讀取資料流
        DataStream<String> source = env.readFile(stringFileInputFormat, path);

        source.print("一般");

        // shuffle 隨機打亂分配
        source.shuffle().print("shuffle");

        // keyBy 根據指定key哈希分配
        KeyedStream<String, String> keyBy = source.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                String[] split = value.split(",");
                return split[0];
            }
        });
        keyBy.print("keyBy");

        // global 全部放到第一個分區
        source.global().print("global");

        env.execute("PartitionStream");

    }

}
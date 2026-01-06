package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

//從檔案讀取
public class WordCount {

    public static void main(String[] args) throws Exception {

        String path = "/Users/sai/IdeaProjects/Flink/inputData/data.txt";

        // 建立 Flink 流式執行環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 自訂 FileInputFormat 來從檔案讀取每一行資料
        FileInputFormat<String> myFileInputFormat = new FileInputFormat<String>(new Path(path)) {
            private transient BufferedReader reader;
            private transient String nextLine;

            // 初始化讀檔環境
            @Override
            public void open(FileInputSplit split) throws IOException {
                super.open(split);
                // 初始化 BufferedReader，用來從檔案流中讀取文字
                reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
                // 讀取檔案的第一行，作為下一次輸出的資料
                nextLine = reader.readLine();
            }

            // 告訴 Flink 讀到檔案尾端了嗎
            @Override
            public boolean reachedEnd() throws IOException {
                // 判斷檔案是否已經讀完
                // 當 nextLine 為 null 時，表示檔案已到結尾
                return nextLine == null;
            }

            // 每次提供下一筆資料給 Flink
            @Override
            public String nextRecord(String reuse) throws IOException {
                // 取得當前行作為本次輸出
                String currentLine = nextLine;
                // 讀取檔案下一行，預先存到 nextLine，以便下次呼叫
                nextLine = reader.readLine();
                // 回傳當前行
                return currentLine;
            }

            // 釋放資源
            @Override
            public void close() throws IOException {
                super.close();
                // 關閉 BufferedReader，釋放資源
                if (reader != null) {
                    reader.close();
                }
            }

        };

        // 從檔案讀取資料流
        DataStream<String> source = env.readFile(myFileInputFormat, path);

        // flatMap：將每一行拆成 (word, 1)
        DataStream<Tuple2<String, Integer>> words = source.flatMap(new MyFlatMapFunction());

        // keyBy + reduce：依照 word 累加
        DataStream<Tuple2<String, Integer>> result = words
                .keyBy(value -> value.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) {
                        return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
                    }
                });

        // 輸出結果
        result.print();

        // 執行任務
        env.execute("Word Count Example");
    }

    // flatMap 將文字拆成單字
    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            if (line == null)
                return; // 避免 null
            for (String word : line.split("\\s+")) {
                if (!word.isEmpty()) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}

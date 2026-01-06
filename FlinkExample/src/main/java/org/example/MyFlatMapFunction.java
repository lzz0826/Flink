package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

// flatMap：把每一行文字拆成單字，並轉成 (word, 1)
public class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
        List<String> list = Arrays.asList(in.split(" "));
        for (String word : list) {
            if (!word.trim().isEmpty()) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}

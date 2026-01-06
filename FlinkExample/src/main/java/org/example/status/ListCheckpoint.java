package org.example.status;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.utils.MyFileInputFormat;

import java.util.Collections;
import java.util.List;

//算子狀態
public class ListCheckpoint {

    public static void main(String[] args) throws Exception {

        String path = "/Users/sai/IdeaProjects/Flink/inputData/data2.txt";

        // 建立 Flink 環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 必須開啟 Checkpointing，否則不會觸發 snapshotState()
        env.enableCheckpointing(3000); // 每 3 秒做一次 checkpoint

        FileInputFormat<String> inputFormat = new MyFileInputFormat(new Path(path));

        // 從檔案讀取資料
        DataStream<String> source = env.readFile(inputFormat, path);

        source
                .map(new MyListCheckpointMapFunction())
                .print();

        env.execute();
    }

    /**
     * 這個 MapFunction 同時實作 ListCheckpointed，用於示範「Operator State」的 checkpoint 與恢復。
     *
     * ListCheckpointed<T> 適合儲存小型 list 狀態（如 counter、buffer 等）。
     */
    public static class MyListCheckpointMapFunction
            implements MapFunction<String, Integer>, ListCheckpointed<Integer> {

        /**
         * Operator 的本地狀態（不會自動分散，是每個 operator instance 自己的）
         * 用於記錄本 operator 已經處理過多少筆資料。
         */
        private Integer count = 0;

        /**
         * map() 每處理一筆資料就把 count + 1
         */
        @Override
        public Integer map(String value) {

            // 處理筆數累計
            count++;

            System.out.println("處理第 " + count + " 筆資料：" + value);

            // 回傳字串長度（僅示範用）
            return value.length();
        }

        /**
         * snapshotState：
         * Flink 觸發 checkpoint 時會呼叫此方法
         * 將現在 operator 的狀態存成一個 List<T>
         */
        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) {
            System.out.println("=== snapshotState: count=" + count + " ===");
            // ListCheckpointed 必須回傳一個 list
            return Collections.singletonList(count);
        }

        /**
         * restoreState：
         * operator 在失敗後恢復時，會呼叫此方法
         * 傳入的 state 是之前 snapshotState 存下的值
         */
        @Override
        public void restoreState(List<Integer> state) {
            // ===============================
            // restoreState 說明：
            // -------------------------------
            // Flink 在 checkpoint 恢復 / 程式重啟 / rescale（並行度變化）時，
            // 會把所有舊 subtask（舊分區）的 Operator State 傳進來。
            //
            // List<Integer> state：
            //   - 每個元素代表「一個舊 subtask 的 state」
            //   - 並行度變少 → 會收到多份舊 state，需要合併
            // ===============================

            int recovered = 0;

            // 將多個舊 subtask 的 state 合併
            for (Integer num : state) {
                recovered += num;
            }

            // 將合併後的狀態真正寫回 operator 的本地狀態
            this.count = recovered;

            System.out.println("=== restoreState: restored count=" + count + " ===");
        }

    }
}

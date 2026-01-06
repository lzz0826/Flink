package org.example.status;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.MyObservation;
import org.example.utils.MyFileInputFormat;


/*
* **奘態後端（State Backend）**就是 Flink 存放運行時狀態（keyed state / window state / operator state）的容器。
小狀態 → 可能存在記憶體（MemoryStateBackend）
大狀態 → 存磁碟（RocksDBStateBackend）
配合 checkpoint 可以實現 故障恢復
奘態後端 = Flink 的狀態存放器 + 容錯基礎。
* */
public class StateBackend {

    public static void main(String[] args) throws Exception {

        String path = "/Users/sai/IdeaProjects/Flink/inputData/data2.txt";

//        | Backend                            | 特點                                 | 適用場景                              |
//        | --------------------------------   | -------------------------------     | ------------------------------       |
//        | **Memory / HashMapStateBackend**   | State 全部在 JVM 記憶體               | 小量資料、測試環境                      |
//        | **FileSystemStateBackend（舊）**    | State 在記憶體，Checkpoint 在 HDFS/S3 | 已被新 API 取代                        |
//        | **RocksDBStateBackend（現代標準）**  | 大量 state 一律存在 RocksDB（磁碟）     | 大流量、production、GB ~ TB 級 state   |

        // --- 1. 設定 RocksDB(嵌入式庫) backend + checkpoint 目錄 *把奘態後端存在 RocksDB 確保數據不會消失---
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb"); // 設定使用 RocksDB 存狀態（所有 keyed state, window state 都會存在 RocksDB）
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");// 設定 checkpoint 存儲方式，這裡使用本機檔案系統
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///Users/sai/IdeaProjects/Flink/src/main/java/org/example/status");// checkpoint 存放路徑（出錯時 Flink 可以從這個目錄回滾）
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);// 開啟 RocksDB 增量 checkpoint，可減少磁碟寫入量（非必須）

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(4);

        //-------- 檢查點配置 --------
        env.enableCheckpointing(300);// 每 300 ms 觸發一次 checkpoint（一般建議秒級，但你這邊應該只是測試）
        // 檢查點進階設定
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingConsistencyMode(org.apache.flink.core.execution.CheckpointingMode.EXACTLY_ONCE);// EXACTLY_ONCE：最強語意，確保每筆資料只處理一次
        checkpointConfig.setCheckpointTimeout(60000L);// 如果 checkpoint 超過 60 秒還沒完成，就認定 timeout
        checkpointConfig.setMaxConcurrentCheckpoints(2);// 同時最多允許執行 2 個 checkpoint（並行 checkpoint）
        checkpointConfig.setMinPauseBetweenCheckpoints(60000L);// 兩次 checkpoint 中間至少間隔 60 秒避免過度佔用 IO 並確保每個 checkpoint“間隔”
        checkpointConfig.setTolerableCheckpointFailureNumber(0);// 允許的 checkpoint 失敗次數（0 = 不允許失敗，一失敗 job 就 fail）

        // --- 啟用 checkpoint 紀錄 奘態後端---
        // 每 10 秒做一次 checkpoint，保證 job 崩掉後可以恢復（exactly-once）
//        Flink 會從最新的 checkpoint 恢復：
//        RocksDB 會還原成 checkpoint 時的狀態
//        job 會從上次事件時間 / source offset 繼續
        env.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000L);


        FileInputFormat<String> input = new MyFileInputFormat(new Path(path));

        // 讀檔
        DataStream<String> source = env.readFile(input, path);

        // 轉換成物件
        DataStream<MyObservation> parsed = source.map(new MapFunction<String, MyObservation>() {
            @Override
            public MyObservation map(String line) throws Exception {

                String[] split = line.split(",");

                MyObservation m = new MyObservation();
                m.setId(Integer.parseInt(split[0]));
                m.setName(split[1]);
                m.setTemperature(Double.parseDouble(split[2]));
                m.setTime(Long.parseLong(split[3]));
                return m;
            }
        });

        KeyedStream<MyObservation, Integer> keyed = parsed.keyBy(MyObservation::getId);

        keyed.map(new RichMapFunction<MyObservation, String>() {

            // 使用 ValueState<Double> 存每個 key 的累積溫度 ---
            private transient ValueState<Integer> myState;
            @Override
            public void open(OpenContext openContext) throws Exception {
                // 定義狀態描述器
                ValueStateDescriptor<Integer> descriptor =
                        new ValueStateDescriptor<>("myState", Integer.class, 0); // 初始值 0
                myState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public String map(MyObservation value) throws Exception {
                // 這裡就是手動放東西進 state
                int current = myState.value();
                current += value.getTemperature(); // 累加
                myState.update(current); // 更新到 RocksDB
                return "key=" + value.getId() + " state=" + current;
            }
        }).print();

        env.execute("FaultTolerance Example");
    }
}

package org.example.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.example.MyObservation;
import org.example.utils.MyFileInputFormat;

import java.time.Duration;

// ---------------------------------------------
// Watermark（水位線）設定原則
// ---------------------------------------------

// 1. 分析來的資料亂序程度
//    - 先觀察資料的 eventTime 與實際到達時間差多少
//    - 例如 95% 的資料延遲在 3 秒以內
//    - Watermark 就可以設為 3 秒左右（允許 3 秒亂序）

// 2. 延遲容忍度 vs 資料準確度（Trade-off）
//    - Watermark 設越大 → 窗口越慢觸發 → 結果越準確
//    - Watermark 設越小 → 反應越快 → 但亂序資料容易被視為遲到
//    - 工程上通常依 95% / 99% 延遲量決定

// 3. OutputTag（側輸出）作為補救機制
//    - 如果資料延遲超過 Watermark 設定 → 被視為遲到資料
//    - 使用 sideOutputLateData(OutputTag) 可以接住這些資料
//    - 常見做法：正常資料 + 遲到資料合併補算 or 落地補償

// ---------------------------------------------
// 常見設定模式：
// new BoundedOutOfOrdernessTimestampExtractor(Duration.ofSeconds(X))
// X = 根據資料最大可接受亂序時間（例如 2 秒、3 秒、5 秒）
// ---------------------------------------------

public class FlinkWatermarkExample {

    public static void main(String[] args) throws Exception {

        String path = "/Users/sai/IdeaProjects/Flink/inputData/data2.txt";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        // 遲到資料的 OutputTag
        final OutputTag<MyObservation> LATE_TAG = new OutputTag<MyObservation>("late-data"){};

        // 先設定 Timestamp + Watermark
        DataStream<MyObservation> withWatermark =
                parsed.assignTimestampsAndWatermarks(

                        // 升序數據設置事件時間
//                        new  AscendingTimestampExtractor<MyObservation>() {
//                            @Override
//                            public long extractAscendingTimestamp(MyObservation myObservation) {
//                                return myObservation.getTime(); // 事件時間欄位
//                            }
//                        }

                        //亂序數據設置事件時間 出發計算
                        new BoundedOutOfOrdernessTimestampExtractor<MyObservation>(
                                Duration.ofSeconds(2) //允許亂序 2 秒
                        ) {
                            @Override
                            public long extractTimestamp(MyObservation m) {
                                return m.getTime(); // 事件時間欄位
                            }
                        }
                        );

        // 分組
        SingleOutputStreamOperator<MyObservation> windowResult = withWatermark
                .keyBy(MyObservation::getId)
                // 10 秒事件時間窗口
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                // 放置過度延遲資料
                .sideOutputLateData(LATE_TAG)
                .sum("temperature");

        windowResult.print("WINDOW RESULT");

        // 取出遲到資料 可以依照業務做補救
        windowResult.getSideOutput(LATE_TAG).print("LATE DATA");

        env.execute("Watermark Example");
    }
}

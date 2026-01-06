package org.example.flinkSql;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.example.MyObservation;
import org.example.utils.MyFileInputFormat;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.table.api.Expressions.$;

//FlinkSql 用SQL語法去操作
public class flinkSql {

    public static void main(String[] args) throws Exception {
        String path = "/Users/sai/IdeaProjects/Flink/inputData/data2.txt";

        // StreamTableEnvironment 流轉Table 後 Sql查詢
        StreamTableEnvironment(path);

        //分組窗口 使用 Table 加上時間窗口 Sql
//        TableGroupWindow(path);

        //聚合窗口 使用 Table 加上 事件時間排序 窗口行數 Sql
//        TableOverWindow(path);

        //使用TableDescriptor描述器來讀取文件 Sql
//        TableDescriptorFilesResource(path);

        //使用TableDescriptor描述器來寫入文件
//        TableDescriptorToFiles();

        //使用TableDescriptor描述器 Kafka 作為數據源 Sql
//        TableDescriptorSourceKafka("input_topic");

        //使用TableDescriptor描述器  向Kafka 推送 Sql
        TableDescriptorPushKafka("output_topic");

    }



    //使用TableDescriptor描述器 Kafka 作為數據源
    //測試連接:
//    docker exec -it kafka kafka-console-producer \
//            --broker-list localhost:9092 \
//            --topic input_topic
    //測試資料:
    //0003,equipment3,11.6,1761524214
    public static void TableDescriptorSourceKafka(String topic) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //創建一個 Kafka 的臨時表
        tableEnv.createTemporaryTable(
                topic, // 表名，之後 SQL 可以直接使用
                TableDescriptor.forConnector("kafka") // 這裡使用 Kafka connector（讀取 Kafka topic）
                        .schema(Schema.newBuilder()   // 定義表的 schema
                                .column("id", DataTypes.INT())          // 欄位 id -> int
                                .column("name", DataTypes.STRING())     // 欄位 name -> string
                                .column("temperature", DataTypes.DOUBLE()) // 欄位 temperature -> double
                                .column("ts", DataTypes.BIGINT())       // 欄位 ts -> long（timestamp）
                                .build()
                        )
                        .option("topic", topic)// 指定 Kafka topic 名稱
                        .option("properties.bootstrap.servers", "localhost:9092") // Kafka broker 地址
                        .option("scan.startup.mode", "latest-offset")// 從最新的 offset 開始讀（如果是 earliest 就會從最早消息開始讀）
                        .format("csv")// 設定 Kafka 消息的格式為 CSV（需要 Flink CSV 依賴）
                        .build()
        );

        Table result = tableEnv.sqlQuery("SELECT * FROM input_topic");
        tableEnv.toChangelogStream(result).print(); // 將 Table 轉換成 changelog 流（INSERT/UPDATE/DELETE 事件）

        env.execute();
    }

    //使用TableDescriptor描述器  向Kafka 推送
    //連接測試:
//    docker exec -it kafka kafka-console-consumer \
//            --bootstrap-server localhost:9092 \
//            --topic output_topic \
//            --from-beginning
    public static void TableDescriptorPushKafka(String topic) throws ExecutionException, InterruptedException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// 創建 Kafka Sink 表
        tableEnv.createTemporaryTable(
                topic,
                TableDescriptor.forConnector("kafka")
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .column("temperature", DataTypes.DOUBLE())
                                .column("ts", DataTypes.BIGINT())
                                .build()
                        )
                        .option("topic", topic)
                        .option("properties.bootstrap.servers", "localhost:9092")
                        .format("csv")
                        .build()
        );

        // 將單條資料轉成 DataStream
        DataStream<Row> rowStream = env.fromElements(
                Row.of(9991, "equipment999", 55.1, 1762392442L)
        );

        // 將 DataStream 轉成 Table
        Table table = tableEnv.fromDataStream(rowStream);

        // 寫入 Kafka Sink 表
        TableResult outputTopic = table.executeInsert("output_topic");


        outputTopic.await();  // 如果想等待 job 完成

        // 啟動 Flink 流程
//        env.execute();
    }

    public static void TableDescriptorFilesResource(String path) throws Exception {

        // 建立 Flink 流式執行環境（StreamExecutionEnvironment）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 建立 Table API / SQL 的執行環境（TableEnvironment）
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 建立一個名為 "sensor" 的臨時表
        tableEnv.createTemporaryTable(
                "sensor", // 表名（之後 SQL 會用到這個名稱）
                TableDescriptor.forConnector("filesystem")  // 使用 "filesystem" connector（讀文件）描述器
                        .schema(Schema.newBuilder()        // 定義表的 schema（欄位結構）
                                .column("id", DataTypes.INT())              // 第一欄：id（int）
                                .column("name", DataTypes.STRING())         // 第二欄：name（string）
                                .column("temperature", DataTypes.DOUBLE())  // 第三欄：temperature（double）
                                .column("time", DataTypes.BIGINT())         // 第四欄：time（bigint）
                                .build()
                        )
                        .option("path", "file:///Users/sai/IdeaProjects/Flink/inputData/data2.txt")// 指定要讀取的檔案路徑（本地路徑要加 file:///）
                        .format("csv")// 指定格式為 CSV（需要 flink-csv 依賴）
                        .build()
        );

        Table result = tableEnv.sqlQuery("SELECT * FROM sensor");

        tableEnv.toChangelogStream(result).print(); // 將 Table 轉換成 changelog 流（INSERT/UPDATE/DELETE 事件）

        env.execute();
    }

    //輸出到文件
    public static void TableDescriptorToFiles() throws Exception {

        // 建立 Flink 流式執行環境（StreamExecutionEnvironment）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 建立 Table API / SQL 的執行環境（TableEnvironment）
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 建立一個名為 "sensor" 的臨時表
        tableEnv.createTemporaryTable(
                "sensor", // 表名（之後 SQL 會用到這個名稱）
                TableDescriptor.forConnector("filesystem")  // 使用 "filesystem" connector（讀文件）描述器
                        .schema(Schema.newBuilder()        // 定義表的 schema（欄位結構）
                                .column("id", DataTypes.INT())              // 第一欄：id（int）
                                .column("name", DataTypes.STRING())         // 第二欄：name（string）
                                .column("temperature", DataTypes.DOUBLE())  // 第三欄：temperature（double）
                                .column("time", DataTypes.BIGINT())         // 第四欄：time（bigint）
                                .build()
                        )
                        .option("path", "file:///Users/sai/IdeaProjects/Flink/outputData")// 指定要輸出的檔案路徑（本地路徑要加 file:///）
                        .format("csv")// 指定格式為 CSV（需要 flink-csv 依賴）
                        .build()
        );

        // 將單條資料轉成 DataStream
        DataStream<Row> rowStream = env.fromElements(
                Row.of(9991, "equipment999", 55.1, 1762392442L)
        );

        // 將 DataStream 轉成 Table
        Table table = tableEnv.fromDataStream(rowStream);

        //輸出到文件
        table.executeInsert("sensor");

//        env.execute();
    }

    //StreamTableEnvironment 流轉Table 後 Sql查詢
    public static void StreamTableEnvironment(String path) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MyFileInputFormat format = new MyFileInputFormat(new Path(path));
        DataStream<String> sourceStream = env.readFile(format, path);

        // 轉換成JAVA物件
        DataStream<MyObservation> myObservationDataStream = sourceStream.map(new MapFunction<String, MyObservation>() {
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


        // 建立 StreamTableEnvironment，用來跑 SQL / Table API
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 將 DataStream<MyObservation> 轉成 Flink Table
        Table table = tableEnv.fromDataStream(myObservationDataStream);

        // 把 Table 註冊成暫時的 SQL View
        tableEnv.createTemporaryView("obs", table);


//                | 特性               | toDataStream      | toChangelogStream          |
//                | --------------    | -------------      | -------------------------- |
//                | 支援更新 UPDATE     | 不支援             | 支援                       |
//                | 支援刪除 DELETE     | 不支援             | 支援                       |
//                | 只能 append-only   | ✔是               | 否                        |
//                | 輸出格式            | 普通 Row          | Changelog Row（+I/-U/+U/-D） |
//                | 適用場景            | insert-only 表    | 有聚合、join、window 的表         |

//        Table result = tableEnv.sqlQuery(
//                "SELECT id, name, temperature FROM obs WHERE name = 'equipment1'"
//        );
        // 再轉回 DataStream 並印出結果（實際輸出運算結果）
//        tableEnv.toDataStream(result).print();


        Table result = tableEnv.sqlQuery(
                "SELECT id, count(id) as cid, avg(temperature) as tempAvg FROM obs GROUP BY id"
        );

        // 印出 Table 的 Schema（欄位型態）
        result.printSchema();

        // 再轉回 DataStream 並印出結果（實際輸出運算結果）
        tableEnv.toChangelogStream(result).print();
//        打印結果: I新的一筆 U更新
//        +I[1, 1, 34.1]                    // 第一筆，count=1, avg=34.1
//        -U[1, 1, 34.1]                    // 準備更新
//        +U[1, 2, 33.3]                    // 第二筆加入後 count=2, avg=33.3
//        -U[1, 2, 33.3]
//        +U[1, 3, 38.6]                    // 第三筆後 count=3 avg=38.6
//        -U[1, 3, 38.6]
//        +U[1, 4, 37.575]                  // 第四筆後 count=4


        env.execute();
        //orm 寫法
//        Table result = table.select(
//                Expressions.$("id"),
//                Expressions.$("name"),
//                Expressions.$("temperature"),
//                Expressions.$("time")
//        ).where(Expressions.$("name").isEqual("equipment1"));
    }


    //分組窗口 使用 Table 加上時間窗口
    public static void TableGroupWindow(String path) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MyFileInputFormat format = new MyFileInputFormat(new Path(path));
        DataStream<String> sourceStream = env.readFile(format, path);

        // 轉換為 Java 物件
        DataStream<MyObservation> myObservationDataStream = sourceStream.map(new MapFunction<String, MyObservation>() {
            @Override
            public MyObservation map(String in) throws Exception {
                String[] split = in.split(",");
                MyObservation myObservation = new MyObservation();
                myObservation.setId(Integer.valueOf(split[0]));
                myObservation.setName(split[1]);
                myObservation.setTemperature(Double.parseDouble(split[2]));
                myObservation.setTime(Long.parseLong(split[3]));  // epoch millis
                return myObservation;
            }
        });


        //加上 Watermark（5秒亂序容忍）＋ 設定事件時間欄位（time）
        //    這代表資料可能會延遲 5 秒到達
        DataStream<MyObservation> eventTimeStream = myObservationDataStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<MyObservation>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<MyObservation>() {
                                    @Override
                                    public long extractTimestamp(MyObservation element, long recordTimestamp) {
                                        return element.getTime(); // 事件時間來自 time 欄位
                                    }
                                })
                );

        // ---- Table Environment ----
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // ---- DataStream → Table（重命名 time → ts 並變成事件時間欄位）----
        Table table = tableEnv.fromDataStream(
                eventTimeStream,
                $("id"),
                $("name"),
                $("temperature"),
                $("ts").rowtime()// 重命名 time → ts 並變成事件時間欄位避免保留字段 關鍵：事件時間欄位
        );

        tableEnv.createTemporaryView("obs", table);

        System.out.println("=== 表格 Schema ===");
        table.printSchema();

        /*滾動窗口（TUMBLE）
        | 用途               | 函式                                        | 意義                   |
        | ------------       | ----------------------------------------  | -------------------- |
        | **切分視窗（分組）** | `TUMBLE(ts, INTERVAL '10' SECOND)`         | 只是用來 GROUP BY，不會輸出欄位 |
        | **輸出視窗開始時間** | `TUMBLE_START(ts, INTERVAL '10' SECOND)`   | 回傳視窗開始時間             |
        | **輸出視窗結束時間** | `TUMBLE_END(ts, INTERVAL '10' SECOND)`     | 回傳視窗結束時間             |

        *滑動窗口（HOP / SLIDE）用於需要重疊視窗的情況，例如每 5 秒統計最近 10 秒的數據。
        | 用途       | 函式                                                            | 意義                  |
        | -------- | ----------------------------------------------------------       | ------------------- |
        | 切分視窗（分組） | `HOP(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND)`       | 每 5 秒滑動一次、視窗長度 10 秒 |
        | 輸出視窗開始時間 | `HOP_START(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND)` | 回傳視窗開始時間            |
        | 輸出視窗結束時間 | `HOP_END(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND)`   | 回傳視窗結束時間            |

        *會話窗口（SESSION） 用於事件間隔不固定，根據閒置時間決定窗口結束。
        | 用途           | 函式                                       | 意義                 |
        | --------      | ----------------------------------------- | ------------------ |
        | 切分視窗（分組） | `SESSION(ts, INTERVAL '5' SECOND')`       | 如果事件間隔大於 5 秒，就開新窗口 |
        | 輸出視窗開始時間 | `SESSION_START(ts, INTERVAL '5' SECOND')` | 回傳視窗開始時間           |
        | 輸出視窗結束時間 | `SESSION_END(ts, INTERVAL '5' SECOND')`   | 回傳視窗結束時間           |

        // ---- SQL Window 聚合 ----
        * SQL 視窗聚合（滾動視窗 10 秒）
         分群：id
         聚合：count, avg
         視窗：依 ts 事件時間，每 10 秒滾動一次
        */
        Table result = tableEnv.sqlQuery(
                "SELECT " +
                        "  id, " +
                        "  COUNT(*) AS cnt, " +
                        "  AVG(temperature) AS tempAvg, " +
                        "  TUMBLE_END(ts, INTERVAL '10' SECOND) AS window_end " + //TUMBLE_END(ts, INTERVAL '10' SECOND)輸出每個窗口的結束時間
                        "FROM obs " +
                        "GROUP BY id, TUMBLE(ts, INTERVAL '10' SECOND)" //用來「定義視窗」並作為 GROUP BY 的鍵 事件時間 ts 按照每 10 秒切成一個個固定長度的窗口。
        );

        System.out.println("=== Window 結果 Schema ===");
        result.printSchema();

        // ---- 輸出結果 ----
        tableEnv.toChangelogStream(result).print();

        env.execute();
    }

    //聚合窗口 使用 Table 加上 事件時間排序 窗口行數
    public static void TableOverWindow(String path) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MyFileInputFormat format = new MyFileInputFormat(new Path(path));
        DataStream<String> sourceStream = env.readFile(format, path);

        // 轉換為 Java 物件
        DataStream<MyObservation> myObservationDataStream = sourceStream.map(new MapFunction<String, MyObservation>() {
            @Override
            public MyObservation map(String in) throws Exception {
                String[] split = in.split(",");
                MyObservation myObservation = new MyObservation();
                myObservation.setId(Integer.valueOf(split[0]));
                myObservation.setName(split[1]);
                myObservation.setTemperature(Double.parseDouble(split[2]));
                myObservation.setTime(Long.parseLong(split[3]));  // epoch millis
                return myObservation;
            }
        });


        //加上 Watermark（5秒亂序容忍）＋ 設定事件時間欄位（time）
        //    這代表資料可能會延遲 5 秒到達
        DataStream<MyObservation> eventTimeStream = myObservationDataStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<MyObservation>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<MyObservation>() {
                                    @Override
                                    public long extractTimestamp(MyObservation element, long recordTimestamp) {
                                        return element.getTime(); // 事件時間來自 time 欄位
                                    }
                                })
                );

        // ---- Table Environment ----
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // ---- DataStream → Table（重命名 time → ts 並變成事件時間欄位）----
        Table table = tableEnv.fromDataStream(
                eventTimeStream,
                $("id"),
                $("name"),
                $("temperature"),
                $("ts").rowtime()// 重命名 time → ts 並變成事件時間欄位避免保留字段 關鍵：事件時間欄位
        );

        tableEnv.createTemporaryView("obs", table);

        System.out.println("=== 表格 Schema ===");
        table.printSchema();

        // 8️ 使用 Over Window SQL 計算每個 id 的滑動聚合

        //    OVER ow: 套用名為 ow 的窗口規則 對定義的窗口進行計算，滑動式聚合
        //    WINDOW ow AS: 定義窗口規則（分組、排序、範圍）
        //    PARTITION BY: 將資料按 id 分組，每個 id 是一個獨立窗口分區
        //    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW: 表示每行往前數 2 行 + 當前行作為窗口
        Table result = tableEnv.sqlQuery(
                "SELECT " +
                        " id, " +
                        " COUNT(id) OVER ow AS cnt, " +          // 每行窗口內計算 id 出現次數
                        " AVG(temperature) OVER ow AS avgTemp " + // 每行窗口內計算平均溫度
                        "FROM obs " +
                        "WINDOW ow AS (" +
                        " PARTITION BY id " +                    // 按 id 分組
                        " ORDER BY ts " +                        // 按事件時間排序
                        " ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)" // 最近 2 行 + 當前行
        );
        System.out.println("=== Window 結果 Schema ===");
        result.printSchema();

        // ---- 輸出結果 ----
        tableEnv.toChangelogStream(result).print();

        env.execute();
    }

}













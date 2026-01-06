package org.example.mysql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.example.utils.MyFileInputFormat;
import org.example.MyObservation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


//Flink 存到Mysql
public class FlinkToMysqlSendExample {

    public static void main(String[] args) throws Exception {

        String path = "/Users/sai/IdeaProjects/Flink/inputData/data2.txt";

        // 建立 Flink 環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        FileInputFormat<String> stringFileInputFormat = new MyFileInputFormat(new Path(path));;

        // 從檔案讀取資料流
        DataStream<String> source = env.readFile(stringFileInputFormat, path);

        // 轉換成JAVA物件
        DataStream<MyObservation> myObservationDataStream = source.map(new MapFunction<String, MyObservation>() {
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

        myObservationDataStream.addSink(
                new MysqlSinkFunction(
                        "jdbc:mysql://localhost:13306/test?useSSL=false&serverTimezone=UTC",
                        "root",
                        "tony0204"));

        env.execute("Flink 2.1.0 → Mysql REST");
    }

    public static class MysqlSinkFunction extends RichSinkFunction<MyObservation> {

        private final String url;
        private final String userName;// elastic
        private final String password;

        private Connection conn;
        // SQL預處理
        private PreparedStatement ps;

        // 批次大小（可調）
        private static final int BATCH_SIZE = 200;
        private int batchCount = 0;

        public MysqlSinkFunction(String url, String userName, String password) {
            this.url = url;
            this.userName = userName;
            this.password = password;
        }

        /**
         * open 方法會在 sink 啟動時呼叫一次
         * 用來初始化資源，例如建立 Redis 連線
         */
        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            // 建立 JDBC 連線
            conn = DriverManager.getConnection(url, userName, password);
            conn.setAutoCommit(false); // 重要：自己控制 commit，提升效能
        }

        /**
         * invoke 方法會對每一筆流中的資料呼叫一次
         * value：目前要寫入 Redis 的資料（String[]），value[0] 是 key，value[1] 是 value
         * context：可以取得目前事件時間或處理時間等上下文資訊
         */
        @Override
        public void invoke(MyObservation value) throws Exception {

            String upsertSql = "INSERT INTO admin_admin (id, equipment_name, temperature, time) " +
                    "VALUES (?, ?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE equipment_name = VALUES(equipment_name), " +
                    "temperature = VALUES(temperature), time = VALUES(time)";

            try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
                ps.setString(1, String.valueOf(value.getId()));
                ps.setString(2, value.getName());
                ps.setDouble(3, value.getTemperature());
                ps.setLong(4, value.getTime());
                ps.executeUpdate();
            }

            conn.commit(); // 提交

            // 批次處理需要 注意MYSQL的行鎖和表鎖
            // batchCount++;
            // if (batchCount >= BATCH_SIZE) {
            // conn.commit(); // 提交批次
            // batchCount = 0;
            // }
        }

        /**
         * close 方法會在 sink 停止時呼叫
         * 用來釋放資源，例如關閉 Redis 連線
         */
        @Override
        public void close() throws Exception {
            super.close();

            if (ps != null) {
                ps.executeBatch(); // flush 未滿批次的資料
                ps.close();
            }
            if (conn != null) {
                conn.commit();
                conn.close();
            }
        }

    }
}

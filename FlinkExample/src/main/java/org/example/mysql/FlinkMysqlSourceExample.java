package org.example.mysql;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;
import org.example.MyObservation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class FlinkMysqlSourceExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String query = "SELECT id, equipment_name, temperature, time FROM admin_admin";

        env.addSource(new MysqlSource(
                "jdbc:mysql://localhost:13306/test?useSSL=false&serverTimezone=UTC",
                "root",
                "tony0204",
                query
                )).print();

        env.execute("Flink Elasticsearch Source Example");
    }

    /**
     * 自定義 Mysql Source
     */
    public static class MysqlSource extends RichSourceFunction<MyObservation> {

        private final String url;
        private final String userName;
        private final String password;
        private final String query;

        private volatile boolean isRunning = true;
        private Connection conn;

        public MysqlSource(String url, String userName, String password,String query) {
            this.url = url;
            this.userName = userName;
            this.password = password;
            this.query = query;
        }

        /**
         * open 方法
         * 作用：
         *   - 在 Source 啟動時呼叫一次
         *   - 用來初始化資源，例如建立連線、初始化狀態
         *   - 類似建構函數，但 open 可以取得 Flink 的上下文資訊
         * 呼叫時機：
         *   - Flink 任務啟動時，每個 parallel subtask 都會調用一次 open
         */
        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            conn = DriverManager.getConnection(url, userName, password);
        }

        /**
         * run 方法
         * 作用：
         *   - Source 的核心邏輯都寫在這裡
         *   - 不斷產生數據並通過 ctx.collect() 發送到 DataStream
         *   - 可以使用 while(isRunning) 進行輪詢或持續讀取
         * 呼叫時機：
         *   - open() 之後被 Flink 自動調用
         */
        @Override
        public void run(SourceContext<MyObservation> ctx) throws Exception {

            while (isRunning) {
                try (PreparedStatement ps = conn.prepareStatement(query);
                     ResultSet rs = ps.executeQuery()) {

                    while (rs.next()) {
                        MyObservation obs = new MyObservation();
                        obs.setId(rs.getInt("id"));
                        obs.setName(rs.getString("equipment_name"));
                        obs.setTemperature(rs.getDouble("temperature"));
                        obs.setTime(rs.getLong("time"));

                        ctx.collect(obs);
                    }
                }
                // 每 5 秒輪詢一次
                Thread.sleep(5000);
            }
        }

        /**
         * cancel 方法
         * 作用：
         *   - Flink 任務取消時呼叫
         *   - 用來停止 Source 讀取數據，設置標誌位或釋放中斷資源
         * 呼叫時機：
         *   - 用戶手動取消任務
         *   - 任務失敗重啟時
         */
        @Override
        public void cancel() {
            isRunning = false;
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (Exception ignored) {}
        }

        /**
         * close 方法
         * 作用：
         *   - Source 停止時呼叫
         *   - 用來釋放資源，例如關閉連線、清理緩存
         * 呼叫時機：
         *   - cancel() 或任務正常結束後，Flink 會自動調用 close()
         */
        @Override
        public void close() throws Exception {
            super.close();
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }
}

package org.example.redis;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;
import redis.clients.jedis.Jedis;

//Flink 讀 Redis
public class FlinkRedisSourceExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new RedisSource("localhost",6379)).print();

        env.execute("Flink Redis Source Example");
    }

    /**
     * 自定義 Redis Source
     */
    public static class RedisSource extends RichSourceFunction<String> {
        private String host;
        private int port;
        private volatile boolean isRunning = true;
        private transient Jedis jedis;

        public RedisSource(String host,int port) {
            this.port = port;
            this.host = host;
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
            jedis = new Jedis(host, port);
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
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                // 從 Redis 哈希 "user" 讀取所有資料 Redis裡先要有key不然會報錯:Cannot invoke "Object.toString()" because "record" is null
                String user = jedis.get("user");
                ctx.collect(user);
//                    Map<String, String> userMap = jedis.hgetAll("user");
//                    for (Map.Entry<String, String> entry : userMap.entrySet()) {
//                        ctx.collect(entry.getKey() + " -> " + entry.getValue());
//                    }
                // 每 5 秒拉一次
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
            if (jedis != null) {
                jedis.close();
            }
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
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}

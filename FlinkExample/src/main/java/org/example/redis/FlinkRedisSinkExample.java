package org.example.redis;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;

import org.example.utils.MyFileInputFormat;
import org.example.MyObservation;
import redis.clients.jedis.Jedis;


//Flink 存到 Redis
public class FlinkRedisSinkExample {
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

        // 將資料轉成 key/value 格式
        DataStream<String[]> kvStream = myObservationDataStream.map(new MapFunction<MyObservation, String[]>() {
            @Override
            public String[] map(MyObservation in) throws Exception {
                String[] split = new String[2];
                split[0] = String.valueOf(in.getId());
                split[1] = String.valueOf(in.getTemperature());
                return split;
            }
        });

        // 使用自定義 Sink
        kvStream.addSink(new MyRedisSink("localhost", 6379));
        env.execute("Flink Redis Sink Example");
    }

    // 自訂 Flink Redis Sink，用來將資料寫入 Redis
    public static class MyRedisSink extends RichSinkFunction<String[]> {
        private String host;
        private int port;
        // Jedis 客戶端，用來連線 Redis
        private transient Jedis jedis;

        public MyRedisSink() {
        }

        public MyRedisSink(String host, int port) {
            this.host = host;
            this.port = port;
        }

        /**
         * open 方法會在 sink 啟動時呼叫一次
         * 用來初始化資源，例如建立 Redis 連線
         */
        @Override
        public void open(OpenContext openContext) throws Exception {
            jedis = new Jedis(this.host, this.port);
            super.open(openContext);
        }

        /**
         * invoke 方法會對每一筆流中的資料呼叫一次
         * value：目前要寫入 Redis 的資料（String[]），value[0] 是 key，value[1] 是 value
         * context：可以取得目前事件時間或處理時間等上下文資訊
         */
        @Override
        public void invoke(String[] value, Context context) {
            jedis.set("user", value[1]);
        }

        /**
         * close 方法會在 sink 停止時呼叫
         * 用來釋放資源，例如關閉 Redis 連線
         */
        @Override
        public void close() throws Exception {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

}

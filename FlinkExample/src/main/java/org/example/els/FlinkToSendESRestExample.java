package org.example.els;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.example.MyObservation;
import org.example.utils.MyFileInputFormat;


//Flink 存到Els
//測試:
//curl -u elastic:123456 \  "http://localhost:9200/words/_search?pretty"
public class FlinkToSendESRestExample {

    public static void main(String[] args) throws Exception {

        String path = "/Users/sai/IdeaProjects/Flink/inputData/data2.txt";

        // 建立 Flink 環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        FileInputFormat<String> stringFileInputFormat = new MyFileInputFormat(new Path(path));

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

        myObservationDataStream.addSink(new ElsSinkFunction("localhost", 9200, "elastic", "123456", "words"));

        env.execute("Flink 2.1.0 → Elasticsearch REST");
    }

    public static class ElsSinkFunction extends RichSinkFunction<MyObservation> {

        private final String host;
        private final int port;
        private final String userName;// elastic
        private final String password;
        private final String index;
        // transient 避免序列化
        private transient RestHighLevelClient client;

        public ElsSinkFunction(String host, int port, String userName, String password, String index) {
            this.host = host;
            this.port = port;
            this.userName = userName;
            this.password = password;
            this.index = index;
        }

        /**
         * open 方法會在 sink 啟動時呼叫一次
         * 用來初始化資源，例如建立 Redis 連線
         */
        @Override
        public void open(OpenContext openContext) throws Exception {
            if (client == null) {
                // ★★★ 加入 Elasticsearch 帳密設定 ★★★
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials(userName, password));

                client = new RestHighLevelClient(
                        RestClient.builder(
                                new HttpHost(host, port, "http"))
                                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                                        .setDefaultCredentialsProvider(credentialsProvider)));
            }
        }

        /**
         * invoke 方法會對每一筆流中的資料呼叫一次
         * value：目前要寫入 Redis 的資料（String[]），value[0] 是 key，value[1] 是 value
         * context：可以取得目前事件時間或處理時間等上下文資訊
         */
        @Override
        public void invoke(MyObservation value) throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(value);
            IndexRequest request = new IndexRequest(index)
                    .source(json, XContentType.JSON);
            client.index(request, RequestOptions.DEFAULT);
        }

        /**
         * close 方法會在 sink 停止時呼叫
         * 用來釋放資源，例如關閉 Redis 連線
         */
        @Override
        public void close() throws Exception {
            if (client != null) {
                client.close();
            }
        }
    }

}

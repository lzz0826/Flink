package org.example.els;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.client.CredentialsProvider;

public class FlinkElsSourceExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ElsSource("localhost",9200,"elastic","123456","words")).print();

        env.execute("Flink Elasticsearch Source Example");
    }

    /**
     * 自定義 Elasticsearch Source
     */
    public static class ElsSource extends RichSourceFunction<String> {

        private final String host;
        private final int port;
        private final String userName;//elastic
        private final String password;
        private final String index;

        public ElsSource(String host, int port, String userName, String password, String index) {
            this.host = host;
            this.port = port;
            this.userName = userName;
            this.password = password;
            this.index = index;
        }
        
        private volatile boolean isRunning = true;
        private transient RestHighLevelClient client;

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
            //建立 client
            if (client == null) {

                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials(userName, password) //
                );

                client = new RestHighLevelClient(
                        RestClient.builder(
                                new HttpHost(host, port, "http")
                        ).setHttpClientConfigCallback(httpClientBuilder ->
                                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                        )
                );
            }
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
            // 查詢 Elasticsearch
            SearchRequest request = new SearchRequest(index);  // ★★★ index name
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.size(1000); // 每次拉 1000 筆
            request.source(builder);

            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            // 把資料一筆一筆 emit 出去
            for (SearchHit hit : response.getHits().getHits()) {

                if (!isRunning) break;

                String json = hit.getSourceAsString();
                ctx.collect(json); // emit 到 DataStream

            }

            // 如果你要一直抓（像 stream），加 while loop：
            // while (isRunning) { ... Thread.sleep(2000); ... }
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
                if (client != null) {
                    client.close();
                }
            } catch (Exception ignored) {
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
            if (client != null) {
                client.close();
            }
        }

    }
}
package org.analysis.dataStreamAPI;

import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.conf.AnalysisConfig;
import org.pojo.*;
import org.utils.FlinkSource;

import java.time.Duration;
import java.util.*;

import static org.conf.ConfigLoader.loadAppConfig;
import static org.utils.TimeUtil.parseTimeToTimestamp;

//熱門頁面統計

//需求:
//從web服務器日誌中 統計實時的熱門訪問頁面
//統計每分鐘的ip訪問量 取出訪問量最大的5個地址 每五秒更新一次

//思路:
//將 apache 服務器日誌中的時間 轉為時間戳 最為Event Time
//構建滑動窗口 窗口長度1分鐘 滑動距離5秒

//數據格式:83.149.9.216 - - 17/05/2015:10:05:43 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-dashboard3.png
//送訊息
//docker exec -it kafka kafka-console-producer \
//        --broker-list localhost:9092 \
//        --topic HotPageAnalysis_topic


public class HotPageAnalysis {

    public static void main(String[] args) throws Exception {
        AnalysisConfig config = loadAppConfig();

        String topic = config.hotPageAnalysis.kafka.topic;
        String group = config.hotPageAnalysis.kafka.group;
        String host = config.hotPageAnalysis.kafka.host;
        String port = config.hotPageAnalysis.kafka.port;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //建立 Kafka Source
        DataStream<String> kafkaSource = FlinkSource.getKafkaSource(env,host,port,topic,group,1);


        //解析有問題的 測輸出流
        final OutputTag<String> DataError_TAG = new OutputTag<String>("DataError-HotPageAnalysis"){};


        // 轉換成物件 有問題會推到 DataError_TAG 測輸出流
        SingleOutputStreamOperator<Apache> processedStream = kafkaSource.process(
                new ApacheLogParserProcess(DataError_TAG)
        );

        // 處理 解析錯誤 測輸出流
//        processedStream.getSideOutput(DataError_TAG).print("HotPageAnalysis - ERROR");

        //自定義的數據流(模擬測試數據)
//        DataStream<Apache> apacheDataStreamSource = env.addSource(new ApacheTestSource(1));



        // 先設定 事件時間欄位 Timestamp + Watermark
        DataStream<Apache> withWatermark =
                processedStream.assignTimestampsAndWatermarks(

                        //**判斷升序還是亂序 目前測試資料是有序 升序
                        //** 降序不適合流式大數據分析 給Flink之前最好先生序排序
                        //** 亂序太嚴重要確保數據正確必須延長時間 會降低速度

                        // 升序數據設置事件時間
                        new  AscendingTimestampExtractor<Apache>() {
                            @Override
                            public long extractAscendingTimestamp(Apache apache) {
                                return apache.getTimestamp(); // 事件時間欄位
                            }
                        }
                        //亂序數據設置事件時間 觸發計算
//                        new BoundedOutOfOrdernessTimestampExtractor<Apache>(
//                                Duration.ofSeconds(2) //允許亂序 2 秒
//                        ) {
//                            @Override
//                            public long extractTimestamp(Apache m) {
//                                return m.getTimestamp(); // 事件時間欄位
//                            }
//                        }
                );

        // 遲到資料的 OutputTag
        final OutputTag<Apache> LATE_TAG = new OutputTag<Apache>("Late-HotPageAnalysis"){};

        //先統計 ip 訪問量
        //最後輸出 ip:訪問量,五個url
        //根據 ip 開窗
        SingleOutputStreamOperator<HotUrlCount> aggregate = withWatermark
                .keyBy(Apache::getIp)
                //事件滑動窗口 窗口大小 1 分鐘，每 5 秒鍾滑動一次 會重疊
                .window(SlidingEventTimeWindows.of(Duration.ofMinutes(1), Duration.ofSeconds(5)))
                .allowedLateness(Duration.ofSeconds(30)) //主流 允許延遲 30 秒 , 超過會到測輸出 遲到的流
                .sideOutputLateData(LATE_TAG) // 處理延數據 超過 watermark + allowedLateness 的才會進來
                //AggregateFunction+ProcessWindowFunction 窗口會在AggregateFunction , ProcessWindowFunction 會等窗口時間到才執行
                .aggregate(new UrlCountAggregator(), new Top5UrlProcessFunction());


        //處理遲到數據
        SideOutputDataStream<Apache> sideOutput = aggregate.getSideOutput(LATE_TAG);

        SingleOutputStreamOperator<String> map = aggregate.map(new MapFunction<HotUrlCount, String>() {
            @Override
            public String map(HotUrlCount value) throws Exception {
                StringBuilder result = new StringBuilder();

                result.append("-----------------------------------\n");
                result.append("ip:"+value.getIp()+"\n");
                result.append("窗口結束時間"+value.getWindowEnd()+"\n");
                result.append("記數"+value.getIpCount()+"\n");
                result.append("url"+value.getUrlList()+"\n");
                result.append("-----------------------------------\n");
                return result.toString();
            }
        });

        //打印最終結果
        map.print("HotPageAnalysis");

        env.execute();
    }

    //自定義模擬測試數據源
    static class ApacheTestSource implements SourceFunction<Apache> {

        List<String> ipList = Arrays.asList("127.0.0.1","192.168.1","172.111.123","168.0.0.1");
        List<String> url = Arrays.asList("/a","/b","/c","/d","/e","/f","/g");

        Random rand = new Random();

        //生產數據的尖閣 毫秒
        private final int delayTime;

        public ApacheTestSource(int delayTime) {
            this.delayTime = delayTime;
        }

        //開關
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Apache> ctx) throws Exception {
            while (isRunning) {
                Apache apache = new Apache();
                apache.setIp(ipList.get(rand.nextInt(ipList.size())));
                apache.setTimestamp(System.currentTimeMillis());
                apache.setUrl(url.get(rand.nextInt(url.size())));
                ctx.collect(apache);
                Thread.sleep(delayTime);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }


    //轉換為 JAVA 物件 有問題推到 側輸出流
    static class ApacheLogParserProcess extends ProcessFunction<String, Apache> {

        private final OutputTag<String> errorTag;

        public ApacheLogParserProcess(OutputTag<String> errorTag) {
            this.errorTag = errorTag;
        }

        @Override
        public void processElement(String line, Context context, Collector<Apache> out) throws Exception {
            try {

                String[] split = line.split(" ");

                // 檢查日誌格式是否正確（例如，確保有足夠的欄位）
                if (split.length < 6) {
                    // 數據格式錯誤，發送到側輸出流
                    context.output(errorTag, "Malformed line (Insufficient fields): " + line);
                    return;
                }
                //數據格式:83.149.9.216 - - 17/05/2015:10:05:43 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-dashboard3.png
                Apache apache = new Apache();
                apache.setIp(split[0]);
                apache.setIdent(split[1]);
                apache.setAuthUser(split[2]);
                apache.setLogTime(split[3]);
                apache.setTimeZone((split[4]));
                apache.setHttpProtocol(split[5]);
                apache.setUrl(split[6]);

                // 轉時間戳作為事件時間
                long timestamp = parseTimeToTimestamp(apache.getLogTime(), apache.getTimeZone());
                apache.setTimestamp(timestamp);
                // 解析成功，發送到主流
                out.collect(apache);


            } catch (Exception e) {
                // 解析或時間轉換失敗，發送到側輸出流
                context.output(errorTag, "Parsing failed for line: " + line + " Error: " + e.getMessage());
                // 建議同時列印錯誤日誌，以便追蹤
                System.err.println("Failed to parse log line: " + line + ". Error: " + e.getMessage());
            }
        }
    }

    //計算總 ip 和 每個URL對應的次數
    static class UrlCountAggregator implements AggregateFunction<Apache, UrlCountAccumulator, UrlCountAccumulator> {

        @Override
        public UrlCountAccumulator createAccumulator() {
            return new UrlCountAccumulator();
        }

        @Override
        public UrlCountAccumulator add(Apache value, UrlCountAccumulator accumulator) {

            // 統計該 URL 的次數
            accumulator.urlCounts.merge(value.getUrl(), 1L, Long::sum);
            // 統計該 IP 的總訪問量
            accumulator.totalIpAccessCount += 1;
            return accumulator;
        }

        @Override
        public UrlCountAccumulator getResult(UrlCountAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public UrlCountAccumulator merge(UrlCountAccumulator a, UrlCountAccumulator b) {
            // 合併兩個分區的 Map 和總數
            b.urlCounts.forEach((url, count) -> a.urlCounts.merge(url, count, Long::sum));
            a.totalIpAccessCount += b.totalIpAccessCount;
            return a;
        }
    }

    //處理前五個RUL
    static class Top5UrlProcessFunction extends ProcessWindowFunction<UrlCountAccumulator, HotUrlCount,  String , TimeWindow>{

        @Override
        public void process(String key, ProcessWindowFunction<UrlCountAccumulator, HotUrlCount, String, TimeWindow>.Context context, Iterable<UrlCountAccumulator> elements, Collector<HotUrlCount> out) throws Exception {

            HotUrlCount hotUrlCount = new HotUrlCount();

            UrlCountAccumulator urlCountAccumulator = elements.iterator().next();

            // 將 Map 轉換為 List 進行排序
            List<Map.Entry<String, Long>> urlList = new ArrayList<>(urlCountAccumulator.urlCounts.entrySet());

            // 排序：按 Count 降序排列 Key:URL  Value:count
            urlList.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

            // 修正邊界錯誤：確保只取列表實際存在的部分 (Math.min(size, 5))
            int topN = Math.min(urlList.size(), 5);

            if (hotUrlCount.getUrlList() == null) {
                hotUrlCount.setUrlList(new ArrayList<>());
            }

            for (int i = 0 ; i < topN ; i++){
                Map.Entry<String, Long> stringLongEntry = urlList.get(i);
                hotUrlCount.getUrlList().add(stringLongEntry.getKey());
            }
            hotUrlCount.setIp(key);
            hotUrlCount.setIpCount(urlCountAccumulator.totalIpAccessCount);
            //取得窗口結束時間
            hotUrlCount.setWindowEnd(context.window().getEnd());
            out.collect(hotUrlCount);
        }

    }

}

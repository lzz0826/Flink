package org.analysis;

//APP市場推廣統計

//需求:
//從埋點日誌中 統計APP 市場推廣的數據指標
//按照不同的推廣渠道 分別統計數據

//思路:
//通過過濾日誌中的用戶行為 按照不同的渠道進行統計
//可以用 process function處理 得到自定義的輸出數據訊息

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.conf.AnalysisConfig;
import org.pojo.Enums.UserBehaviorEnum;
import org.pojo.UserBehavior;
import org.utils.FlinkSource;
import org.utils.KafkaTopicUtil;

import static org.conf.ConfigLoader.loadAppConfig;

public class AppMarketAnalysis {

    public static void main(String[] args) throws Exception {


        //窗口的延遲關閉時間 延遲衝口來等待數據 可以搭配定時器來控制統計數據的緩存

        AnalysisConfig config = loadAppConfig();

        Long allowedLateness = config.appMarketAnalysis.flink.allowedLateness;
        String topic = config.appMarketAnalysis.kafka.topic;
        String group = config.appMarketAnalysis.kafka.group;
        String host = config.appMarketAnalysis.kafka.host;
        String port = config.appMarketAnalysis.kafka.port;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String path = config.appMarketAnalysis.testData;

        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path(path)
                )
                .build();

        DataStream<String> source = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "File Source"
        );
        //建立 Kafka Source
        DataStream<String> kafkaSource = FlinkSource.getKafkaSource(env,host,port,topic,group,1);


        // 轉換成物件
        DataStream<UserBehavior> parsed = kafkaSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String line) throws Exception {

                String[] split = line.split(",");

                UserBehavior userBehavior = new UserBehavior();

                userBehavior.setUserId(Long.parseLong(split[0]));
                userBehavior.setItemId(Long.parseLong(split[1]));
                userBehavior.setCategoryId(Integer.parseInt(split[2]));
                userBehavior.setBehaviorStr(split[3]);
                userBehavior.setBehavior(UserBehaviorEnum.getByValue(split[3]));
                //**目前時間戳是 1511658000 必須*1000 變為毫秒
                userBehavior.setTimestamp(Long.parseLong(split[4]) * 1000L);
                return userBehavior;
            }
        });


        env.execute();
    }
}

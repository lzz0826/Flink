package org.utils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class FlinkSource {

    /**
     * 建立 Kafka Source，並回傳一個以「一筆 Kafka record = 一筆 String」為單位的 DataStream。
     *
     * @param env         Flink {@link StreamExecutionEnvironment}
     * @param host        Kafka broker host（例如 localhost）
     * @param port        Kafka broker port（例如 9092）
     * @param topic       Kafka topic 名稱
     * @param group       Kafka consumer group id
     * @param partitions Kafka topic 的分區數（建議 >= Flink source parallelism）
     * @return            從 Kafka 讀取的 {@link DataStream}，每筆元素為一行字串
     */
    public static DataStream<String> getKafkaSource(StreamExecutionEnvironment env, String host,String port,String topic, String group, int partitions) {
                String bootstrapServers = host + ":" + port;
        // 建立 topic（只會在不存在時建立）
        KafkaTopicUtil.createTopicIfNotExists(
                bootstrapServers,
                topic,
                partitions,      // partitions（建議 >= parallelism）
                (short) 1
        );
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(host + ":" + port)
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );
        return stream;
    }

    /**
     * 建立 File Source，以「一行文字」為單位讀取檔案內容，並回傳對應的 DataStream。
     *
     * @param env      Flink {@link StreamExecutionEnvironment}
     * @param dataPath 檔案路徑（可為單一檔案或目錄）
     * @return         從檔案讀取的 {@link DataStream}，每筆元素為一行字串
     */
    public static DataStream<String> getFileSource(StreamExecutionEnvironment env,String dataPath){
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path(dataPath)
                )
                .build();

        DataStream<String> source = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "File Source"
        );
        return source;
    }

}

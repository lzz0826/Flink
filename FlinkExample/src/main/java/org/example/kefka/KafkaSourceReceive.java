package org.example.kefka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.utils.MyFileInputFormat;


//建立 topic
//docker exec -it kafka kafka-topics \
//        --create \
//        --topic my-topic \
//        --bootstrap-server localhost:9092 \
//        --partitions 1 \
//        --replication-factor 1

//送訊息
//docker exec -it kafka kafka-console-producer \
//        --broker-list localhost:9092 \
//        --topic my-topic

public class KafkaSourceReceive {

    public static void main(String[] args) throws Exception {
        // 接收Kafka消息
        ReceiveKafka();

    }

    public static void SendKafka() throws Exception {
        String path = "/Users/sai/IdeaProjects/Flink/inputData/data2.txt";

        // 建立 Flink 環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        FileInputFormat<String> stringFileInputFormat = new MyFileInputFormat(new Path(path));;

        // 從檔案讀取資料流
        DataStream<String> source = env.readFile(stringFileInputFormat, path);

        // 建立 KafkaSink（新 API）
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("my-topic") // 目標 topic
                                .setValueSerializationSchema(new SimpleStringSchema()) // 序列化
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // 投遞語意
                .build();

        // 使用 addSink 將資料寫入 Kafka
        source.sinkTo(kafkaSink);

        // 執行任務
        env.execute("Flink SendKafka Example");
    }

    public static void ReceiveKafka() throws Exception {
        // 建立 Flink 執行環境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 建立 Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("my-topic")
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 從 source 建立資料流（新 API）
        DataStream<String> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        // 處理資料
        stream.print();

        // 執行任務
        env.execute("Flink ReceiveKafka Kafka Source Example");
    }

}

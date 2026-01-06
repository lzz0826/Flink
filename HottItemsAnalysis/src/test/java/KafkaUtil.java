import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaUtil {

    /**
     * 從指定的 CSV 檔案中讀取每一行內容，並將其作為獨立的訊息推送到 Kafka 的指定 Topic。
     *
     * <p>此方法會逐行同步發送數據，並在發送每條訊息後選擇性地等待確認 (future.get())
     * 以及執行延遲 (Thread.sleep())。</p>
     *
     * 支援附檔: .csv, .tsv, .json, .xml .log, .txt, .out .conf, .ini, .java, .html, .sh
     *
     * @param filePath 檔案的完整路徑 (例如：/path/to/data.csv)
     * @param host Kafka Broker 的主機名稱或 IP 地址 (例如：localhost)
     * @param prot Kafka Broker 服務的埠號 (例如：9092)
     * @param topic Kafka 要發送到的目標 Topic 名稱
     * @param delayMillisecond 發送每條訊息之間的延遲時間 (毫秒)。用於控制發送速率。
     */
    public static void pushLinesToKafka(String filePath, String host, String prot, String topic, int delayMillisecond) {
        // CSV 路徑
        // Kafka 配置
        String bootstrapServers = host+":"+ prot;

        // Kafka Producer 配置
        Properties props = new Properties();

        // Kafka broker 地址，Producer 會連到這個服務器推送訊息
        props.put("bootstrap.servers", bootstrapServers);

        // Key 的序列化器，將 key 轉成 byte[] 傳給 Kafka
        // 如果不用 key，也要設定一個序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Value 的序列化器，將 value（這裡是 CSV 每行字串）轉成 byte[] 傳給 Kafka
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader br = new BufferedReader(new FileReader(filePath))) {

            String line;
            while ((line = br.readLine()) != null) {
                // 將每行推送到 Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
                Future<RecordMetadata> future = producer.send(record);

                // 可選：同步等待確認（確保送到 Kafka）
                future.get();

                System.out.println("Sent: " + line);

                // 間隔幾毫秒
                Thread.sleep(delayMillisecond);
            }

            System.out.println("All lines sent to Kafka.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

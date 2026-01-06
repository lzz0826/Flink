import org.conf.AnalysisConfig;

import static org.conf.ConfigLoader.loadAppConfig;

public class KafkaPushTest {

    public static void main(String[] args) {

//        HotRealTimeItemAnalysisTest();

//        HotPageAnalysisTest();

        PvUvAnalysisAnalysisTest();

    }

    public static void PvUvAnalysisAnalysisTest() {
        AnalysisConfig config = loadAppConfig();
        KafkaUtil.pushLinesToKafka(
                config.pbvUvAnalysisAnalysis.testData,
                config.pbvUvAnalysisAnalysis.kafka.host,
                config.pbvUvAnalysisAnalysis.kafka.port,
                config.pbvUvAnalysisAnalysis.kafka.topic,
                0
        );
    }


    public static void HotPageAnalysisTest() {
        AnalysisConfig config = loadAppConfig();
        KafkaUtil.pushLinesToKafka(
                config.hotPageAnalysis.testData,
                config.hotPageAnalysis.kafka.host,
                config.hotPageAnalysis.kafka.port,
                config.hotPageAnalysis.kafka.topic,
                1
        );
    }

    public static void HotRealTimeItemAnalysisTest() {
        AnalysisConfig config = loadAppConfig();
        KafkaUtil.pushLinesToKafka(
                config.hotRealTimeItemAnalysis.testData,
                config.hotRealTimeItemAnalysis.kafka.host,
                config.hotRealTimeItemAnalysis.kafka.port,
                config.hotRealTimeItemAnalysis.kafka.topic,
                1
        );
    }


}


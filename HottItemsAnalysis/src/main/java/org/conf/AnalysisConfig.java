package org.conf;

/**
 * 對應 YAML 裡 HotRealTimeItemAnalysis 配置
 */
public class AnalysisConfig {

    public HotRealTimeItemAnalysisConfig hotRealTimeItemAnalysis;
    public HotPageAnalysisConfig hotPageAnalysis;
    public AppMarketAnalysisConfig appMarketAnalysis;
    public PvUvAnalysisAnalysisConfig pbvUvAnalysisAnalysis;
    public AdStatisticsByProvinceAnalysisConfig adStatisticsByProvinceAnalysis;
    public LoginLogAnalysisConfig loginFailAlarmAnalysis;
    public OrderLogAnalysisConfig orderResultAlarmAnalysis;


    public static class FlinkConfig {
        public Long allowedLateness; //窗口延遲關閉時間
        public int parallelism;
    }

    public static class KafkaConfig {
        public String host;
        public String port;
        public String topic;
        public String group;
    }

    //------- HotRealTimeItemAnalysisConfig -------
    public static class HotRealTimeItemAnalysisConfig {
        public String testData;
        public KafkaConfig kafka;
        public FlinkConfig flink;
    }
    //------- HotPageAnalysisConfig -------

    public static class HotPageAnalysisConfig {
        public String testData;
        public KafkaConfig kafka;
        public FlinkConfig flink;
    }

    //------- AppMarketAnalysisConfig -------
    public static class AppMarketAnalysisConfig {
        public String testData;
        public KafkaConfig kafka;
        public FlinkConfig flink;
    }

    //------- PvUvAnalysisAnalysisConfig -------
    public static class PvUvAnalysisAnalysisConfig {
        public String testData;
        public KafkaConfig kafka;
        public FlinkConfig flink;
    }

    //------- AdStatisticsByProvinceAnalysisConfig -------
    public static class AdStatisticsByProvinceAnalysisConfig {
        public String testData;
        public KafkaConfig kafka;
        public FlinkConfig flink;
    }


    //------- LoginLogAnalysisConfig -------
    public static class LoginLogAnalysisConfig {
        public String testData;
        public KafkaConfig kafka;
        public FlinkConfig flink;
    }



    //------- OrderLogAnalysisConfig -------
    public static class OrderLogAnalysisConfig {
        public String testData;
        public String testData2;
        public KafkaConfig kafka;
        public FlinkConfig flink;
    }


}

package org.conf;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;

/**
 * 統一的 YAML 配置讀取器
 */
public class ConfigLoader {

    private static final String DEFAULT_FILE = "config.yml";

    /**
     * 讀取 YAML 並映射到 AppConfig POJO
     */
    public static AnalysisConfig loadAppConfig() {
        Yaml yaml = new Yaml();
        try (InputStream in = ConfigLoader.class.getClassLoader().getResourceAsStream(DEFAULT_FILE)) {
            if (in == null) {
                throw new RuntimeException("Cannot find " + DEFAULT_FILE + " in classpath");
            }
            AnalysisConfig config = yaml.loadAs(in, AnalysisConfig.class);
            return config;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config.yml", e);
        }
    }

    /**
     * 讀取 YAML 並返回 Map（如果想動態取 key）
     */
    public static Map<String, Object> loadConfigAsMap() {
        Yaml yaml = new Yaml();
        try (InputStream in = ConfigLoader.class.getClassLoader().getResourceAsStream(DEFAULT_FILE)) {
            if (in == null) {
                throw new RuntimeException("Cannot find " + DEFAULT_FILE + " in classpath");
            }
            return yaml.load(in);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config.yml as Map", e);
        }
    }

    /**
     * 測試方法
     */
    public static void main(String[] args) {
        AnalysisConfig config = loadAppConfig();
        String topic = config.hotRealTimeItemAnalysis.kafka.topic;
        System.out.println(topic);

    }
}

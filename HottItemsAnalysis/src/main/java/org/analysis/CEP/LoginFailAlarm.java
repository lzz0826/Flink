package org.analysis.CEP;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.conf.AnalysisConfig;
import org.pojo.LoginEvent;
import org.pojo.LoginLog;
import org.utils.FlinkSource;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.conf.ConfigLoader.loadAppConfig;

/**
 * 使用 CEP 解決複雜問題  連續登入失敗監控
 *
 * 輸入 -> 規則 -> 輸出
 *
 * 規則：
 * - 同一 userId
 * - 3 秒內
 * - 連續失敗 2 次
 */
public class LoginFailAlarm {

    public static void main(String[] args) throws Exception {

        //失敗次數
        final int failFrequency = 2;

        //區間 秒
        final int IntervalTime = 3;

        //規則名
        final String PatternName = "failPattern";

        AnalysisConfig config = loadAppConfig();
        int parallelism = config.loginFailAlarmAnalysis.flink.parallelism;

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        String testData = config.loginFailAlarmAnalysis.testData;
        DataStream<String> source =
                FlinkSource.getFileSource(env, testData);

        DataStream<LoginLog> parsed =
                source.map(new MapFunction<String, LoginLog>() {
                    @Override
                    public LoginLog map(String line) {
                        String[] split = line.split(",");
                        LoginLog log = new LoginLog();
                        log.setUserId(Long.parseLong(split[0]));
                        log.setIp(split[1]);
                        log.setLoginStatus(split[2]);
                        // 秒 → 毫秒
                        log.setTimestamp(Long.parseLong(split[3]) * 1000L);
                        return log;
                    }
                });

        // EventTime + Watermark（正式 CEP 一定要）
        DataStream<LoginLog> withWatermark =
                parsed.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LoginLog>forBoundedOutOfOrderness(Duration.ofSeconds(1))  // 允許亂序 1 秒
                                .withTimestampAssigner(
                                        (event, ts) -> event.getTimestamp()
                                )
                );

        // 依 userId 分流（CEP 必須）
        KeyedStream<LoginLog, Long> keyedStream =
                withWatermark.keyBy(LoginLog::getUserId);

        // 定義 CEP Pattern
        Pattern<LoginLog, ?> loginFailPattern =
                Pattern.<LoginLog>begin(PatternName)
                        // where 條件 這裡類似過濾
                        .where(new SimpleCondition<LoginLog>() {
                            @Override
                            public boolean filter(LoginLog loginLog) throws Exception {
                                return loginLog.getLoginStatus().equals(LoginLog.LoginStatusFail);
                            }
                        })
                        // times 量詞 匹配 n 次
                        .times(failFrequency)
                        .consecutive()// 必須連續
                        // within 秒內，整個 pattern 必須完成匹配
                        .within(Duration.ofSeconds(IntervalTime));

        // 套用 CEP 並輸出警報事件
        DataStream<LoginEvent> alarmStream =
                CEP.pattern(keyedStream, loginFailPattern)
                        .select(new PatternSelectFunction<LoginLog, LoginEvent>() {
                            @Override
                            public LoginEvent select(Map<String, List<LoginLog>> pattern) {

                                //取得 Pattern
                                List<LoginLog> fails = pattern.get(PatternName);

                                LoginLog first = fails.get(0);
                                LoginLog last = fails.get(fails.size() - 1);

                                LoginEvent event = new LoginEvent();
                                event.setUserId(first.getUserId());
                                event.setFistTimestamp(first.getTimestamp());
                                event.setSecondTimestamp(last.getTimestamp());
                                event.setFailCount((long) fails.size());
                                return event;
                            }
                        });

        // 輸出結果
        alarmStream.print("LOGIN_FAIL_ALARM");

        env.execute("LoginFailAlarm");
    }
}

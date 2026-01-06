package org.analysis.CEP;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.conf.AnalysisConfig;
import org.pojo.*;
import org.utils.FlinkSource;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.conf.ConfigLoader.loadAppConfig;

/**
 * 使用 CEP 解決複雜問題
 *
 * 輸入 -> 規則 -> 輸出
 *
 * 第一階段(訂單流) 訂單支付超時 處理 OrderLog.csv 訂單數據
 * 規則：
 * - 同一 orderId
 * - 出現 CREATE
 * - 30 分鐘內必須 PAY
 * - 未 PAY → timeout
 * - timeout 後 PAY → late data（補償）
 * 展示: Timeout
 *
 * 第二階段(三方收據流) 實時對帳 處理 ReceiptLog.csv 第三方回傳的收據
 * 規則：
 * - 同一 paymentId 支付Id
 * - 處理 PAY後 延遲3秒以內
 * - 支付後必須有 ReceiptLog 流傳來的支付收據*
 * 展示: connect 連接數據流
 */
public class OrderResult {

    public static void main(String[] args) throws Exception {

        AnalysisConfig config = loadAppConfig();
        int parallelism = config.orderResultAlarmAnalysis.flink.parallelism;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parallelism);

        //第一階段(訂單流) 訂單支付超時 處理 OrderLog.csv 訂單數據
        String payLog = config.orderResultAlarmAnalysis.testData;
        final int paymentTimeOut = 30; //支付訂單超時區間 分
        final OutputTag<OrderTimeoutEvent> TIMEOUT_TAG = new OutputTag<>("Pay-timeout") {};//處理 Pattern TimeOut
        final OutputTag<OrderLog> LATE_TAG = new OutputTag<>("Late-OrderResult"){};// 處理第三方晚到的訂單 遲到資料的 OutputTag

        //主要邏輯
        SingleOutputStreamOperator<PaySuccessEvent> payStream = OrderPay(env, payLog,paymentTimeOut,TIMEOUT_TAG,LATE_TAG);

//        payStream.print("OrderResult-Pay-Success");//主流 輸出成功的
//        payStream.getSideOutput(TIMEOUT_TAG).print("OrderResult-Pay-Timeout");//TimeOut側流
//        payStream.getSideOutput(LATE_TAG).print("OrderResult-Late-Timeout"); //測輸出流接收其他情況 處理第三方晚到的訂單

        //第二階段(三方收據流) 實時對帳 處理 ReceiptLog.csv 第三方回傳的收據
        String receipt = config.orderResultAlarmAnalysis.testData2;
        DataStream<ReceiptLog> receiptLogDataStream = OrderReceipt(env,receipt);

        final int second = 3; //延遲的秒數
        final OutputTag<ReceiptTimeoutEven> RECEIPT_TIMEOUT_TAG = new OutputTag<>("Receipt-timeout") {};//處理 Receipt TimeOut

        //合併 用 paymentId 作為key
        SingleOutputStreamOperator<Tuple2<PaySuccessEvent,ReceiptLog>> receiptProcess = payStream
                .keyBy(PaySuccessEvent::getPaymentId)
                //被合併的流也要指定 paymentId 作為key
                .connect(receiptLogDataStream.keyBy(ReceiptLog::getPaymentId))
                .process(new CoProcessFunction<PaySuccessEvent, ReceiptLog, Tuple2<PaySuccessEvent,ReceiptLog>>() {

                    // 保存 Receipt 事件
                    private transient ValueState<ReceiptLog> receiptLogEvent;
                    // 保存 Pay 事件
                    private transient ValueState<PaySuccessEvent> paySuccessEvent;

                    @Override
                    public void open(OpenContext openContext) throws Exception {

                        ValueStateDescriptor<ReceiptLog> descriptor = new ValueStateDescriptor<>("receiptLogEvent", ReceiptLog.class);
                        receiptLogEvent = getRuntimeContext().getState(descriptor);

                        ValueStateDescriptor<PaySuccessEvent> payEvent = new ValueStateDescriptor<>("paySuccessEvent", PaySuccessEvent.class);
                        paySuccessEvent = getRuntimeContext().getState(payEvent);

                    }

                    // Pay 到 檢查是否已經有 Receipt
                    // 有 清緩存 正常打印
                    // 無 需要註冊定時器等延遲的 Receipt
                    @Override
                    public void processElement1(PaySuccessEvent value, CoProcessFunction<PaySuccessEvent, ReceiptLog, Tuple2<PaySuccessEvent, ReceiptLog>>.Context ctx, Collector<Tuple2<PaySuccessEvent, ReceiptLog>> out) throws Exception {

                        if(receiptLogEvent.value() != null) {
                            Tuple2<PaySuccessEvent, ReceiptLog> re= new Tuple2<>();
                            re.f0 = value;
                            re.f1 = receiptLogEvent.value();
                            out.collect(re);
                            //清理資源
                            receiptLogEvent.clear();
                            paySuccessEvent.clear();
                        }else {
                            paySuccessEvent.update(value);
                            //註冊定時器等待 Receipt
                            long timerTs = ctx.timerService().currentProcessingTime() + second * 1000L;
                            ctx.timerService().registerEventTimeTimer(timerTs);//註冊 (事件)定時器
                        }
                    }

                    // Receipt 到 檢查是否已經有 Pay
                    // 有 清緩存 正常打印
                    // 無 需要註冊定時器等延遲的 Pay
                    @Override
                    public void processElement2(ReceiptLog value, CoProcessFunction<PaySuccessEvent, ReceiptLog, Tuple2<PaySuccessEvent, ReceiptLog>>.Context ctx, Collector<Tuple2<PaySuccessEvent, ReceiptLog>> out) throws Exception {
                        if(paySuccessEvent.value() != null) {
                            Tuple2<PaySuccessEvent, ReceiptLog> re= new Tuple2<>();
                            re.f0 = paySuccessEvent.value();
                            re.f1 = value;
                            out.collect(re);
                            //清理資源
                            receiptLogEvent.clear();
                            paySuccessEvent.clear();
                        }else {
                            receiptLogEvent.update(value);
                            //註冊定時器等待 Receipt
                            long timerTs = ctx.timerService().currentProcessingTime() + second * 1000L;
                            ctx.timerService().registerEventTimeTimer(timerTs);//註冊 (事件)定時器
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, CoProcessFunction<PaySuccessEvent, ReceiptLog, Tuple2<PaySuccessEvent, ReceiptLog>>.OnTimerContext ctx, Collector<Tuple2<PaySuccessEvent, ReceiptLog>> out) throws Exception {

                        // 避免重複計時器觸發導致的誤報：只有當其中一個狀態為空，另一個不為空時，才構成超時
                        if (paySuccessEvent.value() != null && receiptLogEvent.value() == null) {
                            // Pay 已到，Receipt 未到 -> 超時
                            ReceiptTimeoutEven receiptTimeoutEven = new ReceiptTimeoutEven();
                            receiptTimeoutEven.setOrderId(paySuccessEvent.value().getOrderId());
                            receiptTimeoutEven.setPaymentId(paySuccessEvent.value().getPaymentId());
                            receiptTimeoutEven.setPay(true); // 設置成 Pay 已到但無 Receipt
                            receiptTimeoutEven.setReceipt(false);
                            receiptTimeoutEven.setTimestamp(timestamp);
                            ctx.output(RECEIPT_TIMEOUT_TAG, receiptTimeoutEven);

                        } else if (paySuccessEvent.value() == null && receiptLogEvent.value() != null) {
                            // Receipt 已到，Pay 未到 -> 超時
                            ReceiptTimeoutEven receiptTimeoutEven = new ReceiptTimeoutEven();
                            receiptTimeoutEven.setPaymentId(receiptLogEvent.value().getPaymentId());
                            receiptTimeoutEven.setPay(false);
                            receiptTimeoutEven.setReceipt(true);
                            receiptTimeoutEven.setTimestamp(timestamp);
                            ctx.output(RECEIPT_TIMEOUT_TAG, receiptTimeoutEven);
                        }

                        // 無論是否超時，定時器觸發後都應清理狀態以釋放資源
                        receiptLogEvent.clear();
                        paySuccessEvent.clear();
                    }
                });
        receiptProcess.print("Receipt-Process");
        receiptProcess.getSideOutput(RECEIPT_TIMEOUT_TAG).print("Receipt-Timeout");//TimeOut側流
        env.execute("OrderResult");


    }

    /*
    * 第一階段(訂單流) 訂單支付超時 處理 OrderLog.csv 訂單數據
    * 回傳主流 : 成功支付的流
    *
    * */
    private static SingleOutputStreamOperator<PaySuccessEvent> OrderPay(StreamExecutionEnvironment env, String testData,int paymentTimeOut,OutputTag<OrderTimeoutEvent> TIMEOUT_TAG,OutputTag<OrderLog> LATE_TAG) throws Exception {

        DataStream<String> source = FlinkSource.getFileSource(env, testData);

        DataStream<OrderLog> parsed =
                source.map(new MapFunction<String, OrderLog>() {
                    @Override
                    public OrderLog map(String line) {
                        String[] split = line.split(",");
                        OrderLog log = new OrderLog();
                        log.setOrderId(Long.parseLong(split[0]));
                        log.setOrderStatus(split[1]);
                        log.setPaymentId(split[2]);
                        // 秒 → 毫秒
                        log.setTimestamp(Long.parseLong(split[3]) * 1000L);
                        return log;
                    }
                });

        // EventTime + Watermark（正式 CEP 一定要）
        DataStream<OrderLog> withWatermark =
                parsed.assignTimestampsAndWatermarks(
                        WatermarkStrategy//AscendingTimestampsWatermarks
                                .<OrderLog>forMonotonousTimestamps()  //升序
                                .withTimestampAssigner(
                                        (event, ts) -> event.getTimestamp()
                                )
                );

        //依照 orderId
        KeyedStream<OrderLog, Long> keyedStream = withWatermark.keyBy(OrderLog::getOrderId);


        // 定義時間限制的 CEP Pattern 符合條件：CREATE → PAY
        Pattern<OrderLog, ?> orderPayPattern =
                Pattern
                        //第一個進來的策略一 放進MAP Key:create
                        .<OrderLog>begin(OrderLog.OrderStatusCreate)
                        .where(new SimpleCondition<OrderLog>() {
                            @Override
                            public boolean filter(OrderLog log) {
                                return OrderLog.OrderStatusCreate.equals(log.getOrderStatus());
                            }
                        })
                        //.next("B")       精確連續匹配（strict contiguity）  **中間不能有其他事件**，必須緊跟前一個事件
                        //.followedBy("B") 松散連續匹配（relaxed contiguity） 中間可以有 **不匹配的其他事件**，只要後面出現 B 就算匹配
                        //第二個進來的策略2 放進MAP Key:pay
                        .followedBy(OrderLog.OrderStatusPay)
                        .where(new SimpleCondition<OrderLog>() {
                            @Override
                            public boolean filter(OrderLog log) {
                                return OrderLog.OrderStatusPay.equals(log.getOrderStatus());
                            }
                        })
                        // 整個流程必須在 30 分鐘內完成
                        .within(Duration.ofMinutes(paymentTimeOut));

        SingleOutputStreamOperator<PaySuccessEvent> resultStream =
                CEP.pattern(keyedStream, orderPayPattern)
                        .sideOutputLateData(LATE_TAG)
                        .select(
                                TIMEOUT_TAG,
                                // timeout 觸發
                                new PatternTimeoutFunction<OrderLog, OrderTimeoutEvent>() {
                                    @Override
                                    //timeoutTimestamp = pattern 中「第一個被匹配事件」的事件時間 + within` 定義的時間視窗
                                    public OrderTimeoutEvent timeout(Map<String, List<OrderLog>> pattern, long timeoutTimestamp) {

                                        //重MAP Key:create 拿到
                                        OrderLog create = pattern.get(OrderLog.OrderStatusCreate).get(0);

                                        OrderTimeoutEvent event = new OrderTimeoutEvent();

                                        event.setOrderId(create.getOrderId());
                                        event.setCreateTimestamp(create.getTimestamp());
                                        event.setTimeOutTimestamp(timeoutTimestamp);
                                        return event;
                                    }
                                },
                                // 成功支付觸發
                                new PatternSelectFunction<OrderLog, PaySuccessEvent>() {
                                    @Override
                                    public PaySuccessEvent select(Map<String, List<OrderLog>> pattern) {

                                        //從MAP Key:create 拿到
                                        OrderLog create = pattern.get(OrderLog.OrderStatusCreate).get(0);

                                        //從MAP Key:pay 拿到
                                        OrderLog pay = pattern.get(OrderLog.OrderStatusPay).get(0);

                                        PaySuccessEvent event = new PaySuccessEvent();
                                        event.setOrderId(create.getOrderId());
                                        event.setPaymentId(pay.getPaymentId());
                                        event.setCreateTimestamp(create.getTimestamp());
                                        event.setSuccessTimestamp(pay.getTimestamp());
                                        return event;
                                    }
                                }
                        );
        return resultStream;
    }

    private static DataStream<ReceiptLog> OrderReceipt(StreamExecutionEnvironment env, String testData){

        DataStream<String> source = FlinkSource.getFileSource(env, testData);

        DataStream<ReceiptLog> parsed =
                source.map(new MapFunction<String, ReceiptLog>() {
                    @Override
                    public ReceiptLog map(String line) {
                        String[] split = line.split(",");
                        ReceiptLog log = new ReceiptLog();
                        log.setPaymentId(split[0]);
                        log.setPayFun(split[1]);
                        // 秒 → 毫秒
                        log.setTimestamp(Long.parseLong(split[2]) * 1000L);
                        return log;
                    }
                });

        // EventTime + Watermark（正式 CEP 一定要）
        DataStream<ReceiptLog> withWatermark =
                parsed.assignTimestampsAndWatermarks(
                        WatermarkStrategy//AscendingTimestampsWatermarks
                                .<ReceiptLog>forMonotonousTimestamps()  //升序
                                .withTimestampAssigner(
                                        (event, ts) -> event.getTimestamp()
                                )
                );
        return withWatermark;
    }

}

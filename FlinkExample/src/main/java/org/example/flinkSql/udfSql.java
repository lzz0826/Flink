package org.example.flinkSql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.util.Collector;
import org.example.MyObservation;

import static org.apache.flink.table.api.Expressions.call;

import static org.apache.flink.table.api.Expressions.$;

public class udfSql {

    public static void main(String[] args) throws Exception {

        // 建立 Flink 環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 測試 Socket : socat -v TCP-LISTEN:9999,reuseaddr,fork -
        // 測試數據範例: 0001,equipment1,29.1,1762392490
        DataStream<String> source = env.socketTextStream("localhost", 9999);

        // 轉換成 Java 物件
        DataStream<MyObservation> myDataStream = source.map(new MapFunction<String, MyObservation>() {
            @Override
            public MyObservation map(String in) throws Exception {
                String[] split = in.split(",");
                MyObservation myObservation = new MyObservation();
                myObservation.setId(Integer.valueOf(split[0]));
                myObservation.setName(split[1]);
                myObservation.setTemperature(Double.parseDouble(split[2]));
                myObservation.setTime(Long.parseLong(split[3]));
                return myObservation;
            }
        });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //定義對應表欄位名
        Table table = tableEnv.fromDataStream(
                myDataStream,
                $("id"),
                $("name"),
                $("temperature")
        );

        //創建表
        tableEnv.createTemporaryView("obs", table);

//                | 功能類型        | ScalarFunction (UDF)                | TableFunction (UDTF)                                |
//                | -----------    | ----------------------------------- | --------------------------------------------------- |
//                | **回傳值**      | 回傳 **一個值（一列一個）**             | 回傳 **多列（0~N 列）**                               |
//                | **使用方式**    | `SELECT myFunc(x)`                  | `LATERAL TABLE(myTableFunc(x))`                     |
//                | **用途**        | 單純做欄位運算：轉換、格式化            | 一筆資料拆成多筆資料                                    |
//                | **實作方式**    | `extends ScalarFunction` + `eval()` | `extends TableFunction<T>` + `eval()` + `collect()` |
//                | **使用時機**    | 產生「一個值」                         | 產生「一張表」                                        |
//                | **SQL 中行為**  | row → scalar                        | row → table                                         |



//        UdtfScalarFunction(tableEnv); //一對一 ScalarFunction 把 sensor 的三個欄位格式化成一段字串。

//        UdfTableFunction(tableEnv); //一對多 TableFunction 輸出成  Tuple2<設備名,設備名長度>

//        MyAggregateFunction(tableEnv); //多對一 AggregateFunction 累計平均溫度

        MyTableAggregateFunction(tableEnv); //多對多 重新排序 溫度由高到低 輸出表

        env.execute();
    }

    //--------------   --------------
    // 自訂義聚合 TableAggregateFunction<聚合的最終結果類型,聚合過程中的中間結果型>
    public static void MyTableAggregateFunction(StreamTableEnvironment tableEnv){
        // 註冊自定方法
        tableEnv.createTemporarySystemFunction("myTableAggregateFunction", MyTableAggregateFunction.class);

        // 取得來源 Table
        Table sourceTable = tableEnv.from("obs");

        // 使用 Table API 進行 Group Table Aggregation
        Table resultTable = sourceTable
                .groupBy($("id")) // 根據 ID 分組
                .flatAggregate(call("myTableAggregateFunction", $("temperature")).as("topTemperature")) // 使用函數拆解
                .select($("id"), $("topTemperature")); // 選擇 ID 和 產出的溫度

        // TableAggregateFunction 不支援寫ＳＱＬ 值些用一般的ＳＱＬ實現
//        String topNQuery =
//                "SELECT id, temperature " +
//                        "FROM (" +
//                        "    SELECT " +
//                        "        id, " +
//                        "        temperature, " +
//                        "        ROW_NUMBER() OVER (PARTITION BY id ORDER BY temperature DESC) AS row_num " + //-- 針對每個 id (PARTITION BY id) 進行分組排名，溫度高的排前面 (DESC)" +
//                        "    FROM obs" +
//                        ") " +
//                        "WHERE row_num <= 2"; // 篩選出排名 1 或 2 的資料

//        Table resultTable = tableEnv.sqlQuery(topNQuery);

        // 輸出結果
        resultTable.execute().print();
    }



    //--------------  AggregateFunction --------------
    // 自訂義聚合 AggregateFunction<聚合的最終結果類型,聚合過程中的中間結果型>
    public static void MyAggregateFunction(StreamTableEnvironment tableEnv){
        // 註冊自定方法
        tableEnv.createTemporarySystemFunction("myAggregateFunction", MyAggregateFunction.class);

        Table sqlAggregateFunction = tableEnv.sqlQuery("SELECT\n" +
                "    id, " +
                "    myAggregateFunction(temperature) AS aggTemperature " +
                "FROM obs " +
                "GROUP BY id"
        );

        //打印表 指定的表 sqlTable
        tableEnv.toChangelogStream(sqlAggregateFunction).print();
    }


    //--------------  TableFunction --------------
    // 自訂義 TableFunction：把 設備名 輸出成  Tuple2<設備名,設備名長度>
    public static void UdfTableFunction(StreamTableEnvironment tableEnv){
        // 註冊自定方法
        tableEnv.createTemporarySystemFunction("myTableFunction", MyTableFunction.class);

        Table sqlTableTableFunction = tableEnv.sqlQuery("SELECT\n" +
                "    id,\n" +
                "    name,\n" +
                "    t.fname AS new_name,\n" +
                "    t.len AS name_len\n" +
                "FROM obs,\n" +
                "LATERAL TABLE(myTableFunction(name)) AS t(fname,len);\n");

        //打印表 指定的表 sqlTable
        tableEnv.toChangelogStream(sqlTableTableFunction).print();
    }

    //-------------- ScalarFunction --------------
    //自訂義 UDF：把 sensor 的三個欄位格式化成一段字串。
    public static void UdtfScalarFunction(StreamTableEnvironment tableEnv){
        // 註冊自定方法
        tableEnv.createTemporarySystemFunction("formatInfo", MyScalarFunction.class);

        //使用 udf 自定義函數
        Table sqlTableScalarFunction = tableEnv.sqlQuery("SELECT " +
                "    id," +
                "    name," +
                "    temperature," +
                "    formatInfo(id, name, temperature) AS info " + //formatInfo(id, name, temperature)參數為自定義的 對應方法eval(Integer id, String name, Double temperature)
                "FROM obs");

        //打印表 指定的表 sqlTable
        tableEnv.toChangelogStream(sqlTableScalarFunction).print();
    }

    /**
     * 自訂義 TableAggregateFunction<聚合的最終結果類型, 聚合過程中的中間結果型>：
     * ---------------------------------------------
     * 輸入：多筆 (Double temperature)
     * 中間狀態：TempAcc (維護最高 & 次高溫度)
     * 輸出：多筆 Double (僅輸出溫度值，不包含 ID)
     *
     * TableAggregateFunction 用來做：
     * 「一組資料產生多筆輸出」
     * 常見用途如：Top N、排名、拆解資料等。
     * * 注意：
     * 由於在 Table API 中使用 .groupBy(id).flatAggregate(...)，
     * 分組鍵 (ID) 已經由 Flink 的 GroupBy 機制保留，
     * 因此這裡只需要輸出「計算後的溫度值」即可。
     */
    public static class MyTableAggregateFunction
            extends TableAggregateFunction<Double, MyTableAggregateFunction.TempAcc> {

        /**
         * 累積器（Accumulator）：
         * ---------------------------------------------------
         * 用來保存「聚合過程中的中間狀態」，
         * 相當於 GROUP BY 過程中的「變數狀態」。
         *
         * 此例需要記錄：
         * - highest：當前最高溫度
         * - second：當前次高溫度
         *
         * 這些狀態會隨著 accumulate() 每筆資料更新。
         */
        public static class TempAcc {
            public Double highest = Double.MIN_VALUE; // 記錄目前組別的最高溫
            public Double second = Double.MIN_VALUE;  // 記錄目前組別的第二高溫
        }

        /**
         * 建立累積器（初始化）
         * ---------------------------------------------------
         * Flink 會在每個 group 開始時呼叫 createAccumulator()
         * 產生一個新的 TempAcc 狀態物件。
         */
        @Override
        public TempAcc createAccumulator() {
            return new TempAcc();
        }

        /**
         * accumulate() 固定的方法名稱
         * public void accumulate(ACC acc, 參數a a, 參數b b...)
         * ---------------------------------------------------
         * 每進來一筆資料時，Flink 會呼叫此方法更新累積器。
         *
         * 邏輯：
         * temp > highest → 置換 highest 並把舊 highest 放去 second
         * temp 介於 highest / second 之間 → 更新 second
         *
         * accumulate() 只負責「更新狀態」，不輸出資料。
         *
         * @param acc   當前累積器（保存最高與次高溫）
         * @param temp  本次輸入的溫度值（由 Table API 傳入）
         */
        public void accumulate(TempAcc acc, Double temp) {
            if (temp > acc.highest) {
                acc.second = acc.highest;   // 原本最高退位到第二高
                acc.highest = temp;          // 新溫度成為最高
            } else if (temp > acc.second) {
                acc.second = temp;           // 更新第二高
            }
        }

        /**
         * emitValue() 固定的方法名稱
         * public void emitValue(ACC acc, Collector<T> out)
         * ---------------------------------------------------
         * 聚合完成後（即該 Group 處理結束時），
         * Flink 會呼叫 emitValue() 來把「多筆結果」輸出出去。
         *
         * 注意：
         * TableAggregateFunction 不是 return，而是用 out.collect()。
         * 你想輸出幾筆就 collect 幾筆。
         *
         * 此例輸出兩筆 Double：
         * 1. highest (最高溫)
         * 2. second (次高溫)
         *
         * @param acc   最終累積器（包含計算好的最高 & 次高溫）
         * @param out   Collector，用來輸出資料到下游
         */
        public void emitValue(TempAcc acc, Collector<Double> out) {
            if (acc.highest > Double.MIN_VALUE) {
                out.collect(acc.highest);
            }
            if (acc.second > Double.MIN_VALUE) {
                out.collect(acc.second);
            }
        }
    }


    //   自訂義聚合 AggregateFunction<聚合的最終結果類型,聚合過程中的中間結果型>
    //這裡的 聚合過程中的中間結果型: Tuple2<平均溫度,計數>
    public static class MyAggregateFunction extends AggregateFunction<Double,Tuple2<Double,Integer>>{


    //初始化 累積器
    @Override
    public Tuple2<Double, Integer> createAccumulator() {
        return new Tuple2<>(0.0,0);
    }

    //最後獲取結果
    @Override
    public Double getValue(Tuple2<Double, Integer> accumulator) {
        return accumulator.f0/accumulator.f1;
    }

    //自己實現中間累加過程 必須實現 固定寫法 accumulate(當前狀態,傳進來的值)
    public void accumulate(Tuple2<Double, Integer> accumulator, Double value){
        //累加當前的溫度 + 下個進來的溫度
        accumulator.f0+=value;

        //增加計數器
        accumulator.f1++;

    }

}


    /**
     * 自訂義 UDF：把 sensor 的三個欄位格式化成一段字串。
     */
    public static class MyScalarFunction extends ScalarFunction {

        /**
         * eval() 是 ScalarFunction 固定的入口方法
         * 這裡示範多參數輸入：id、name、temperature (可以自訂參數)
         * 返回類型就是 eval的返回類型
         */
        public String eval(Integer id, String name, Double temperature) {

            // 1. 參數檢查（傳入 null 時避免 NPE）
            if (id == null || name == null || temperature == null) {
                return "Invalid Sensor Data";
            }

            // 2. 做資料處理（此例簡單拼接成格式化字串）
            String formatted = String.format(
                    "Sensor[id=%d, name=%s, temp=%.2f°C]",
                    id, name, temperature
            );

            // 3. 回傳處理後的結果（此例回傳字串）
            return formatted;
        }
    }

    /**
     * 自訂義 TableFunction：把 設備名 輸出成  Tuple2<設備名,設備名長度>
     * TableFunction 多行輸出
     */
    public static class MyTableFunction extends TableFunction<Tuple2<String, Integer>> {

        /**
         * eval() 是 ScalarFunction 固定的入口方法
         * 沒有返回值 返回是由TableFunction<>范行定義
         */
        public void eval(String name) {

            collect(new Tuple2<>("new:"+name, name.length()));
        }
    }
}

package org.example.window;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.example.MyObservation;

import java.util.Iterator;

/**
 * 自訂 Window Evictor（移除器）

 * Evictor 會在窗口計算「之前」或「之後」移除部分資料。
 * 用法：
 *   .evictor(new MyEvictor())
 *
 * 注意：Evictor 只有在使用一般 Window（而非增量 reduce/aggregate）時才有效。
 *       若你用的是 reduce/aggregate，evictBefore/evictAfter 仍會被呼叫，
 *       但計算方式可能已經是增量模式，不一定能完整控制資料。
 */
public class MyEvictor implements Evictor<MyObservation, TimeWindow> {

    /**
     * evictBefore：
     *   在窗口「開始做計算之前」被執行
     *   可移除視窗中的資料
     *
     * @param elements  視窗內所有元素（包含 timestamp）
     * @param size      視窗目前元素個數
     * @param window    當前視窗資訊 (Start, End)
     * @param ctx       上下文（包含當前時間等資訊）
     */
    @Override
    public void evictBefore(Iterable<TimestampedValue<MyObservation>> elements,
                            int size, TimeWindow window, EvictorContext ctx) {
        // 假設視窗最多只能保留 5 筆資料
        if (size > 5) {
            // 必須用 Iterator 才能在迭代時安全移除資料
            Iterator<TimestampedValue<MyObservation>> it = elements.iterator();
            // it.next() 拿到第一筆資料（視為最舊的一筆）
            // 這裡沒有做比對，單純示範移除「第一筆」
            if (it.hasNext()) {
                it.next();  // 移到第一筆元素
                it.remove(); // 移除該筆資料
            }
        }
    }

    /**
     * evictAfter：
     *   在窗口「計算結束後」執行
     *   可選擇是否再移除資料（例如只保留前 10 筆）
     *
     * @param elements  視窗元素
     * @param size      目前視窗大小
     * @param window    視窗資料
     * @param ctx       上下文
     */
    @Override
    public void evictAfter(Iterable<TimestampedValue<MyObservation>> elements,
                           int size, TimeWindow window, EvictorContext ctx) {
        // Demo 中不處理計算後的清理
        // 若要在 reduce/processWindow 之後刪資料可寫這裡
    }
}

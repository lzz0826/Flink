package org.example.window;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.example.MyObservation;

/**
 * 自訂 Window Trigger（觸發器）
 *
 * Trigger 控制「何時」觸發窗口的計算。
 * 默認 window 只會在窗口時間到時觸發，
 * 但用 Trigger 就可以做到：
 *   每來一筆就觸發
 *   滿 X 筆觸發
 *   特定欄位變化觸發
 *   每秒觸發一次
 *   自己清空視窗資料
 */
public class MyTrigger extends Trigger<MyObservation, TimeWindow> {

    /**
     * onElement：
     *   每進來一筆元素就會執行一次
     *
     * @param element     當前新資料
     * @param timestamp   該資料的事件時間（若有設定）
     * @param window      該資料屬於哪個視窗
     * @param ctx         可用來註冊計時器、存取狀態
     *
     * 回傳值：
     *   - FIRE        → 立刻觸發窗口計算
     *   - CONTINUE    → 不做事，等下一次
     *   - FIRE_AND_PURGE → 觸發後清空視窗資料
     *   - PURGE       → 不觸發，但清空視窗資料
     */
    @Override
    public TriggerResult onElement(MyObservation element, long timestamp, TimeWindow window, TriggerContext ctx) {
        // 每來一筆資料就立即觸發窗口
        return TriggerResult.FIRE;
    }

    /**
     * onProcessingTime：
     *   當 Processing Time 計時器被觸發時執行
     *
     * @param time        -> 哪個 timer 到期
     * @param window      -> 該 timer 所屬窗口
     * @param ctx         -> 用來註冊/刪除 timer
     *
     * 如果你曾在 openElement 註冊 processing timer：
     * ctx.registerProcessingTimeTimer(...)
     *
     * 那 timer 到期就會進來這裡。
     */
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
        // processing time 到時一樣觸發窗口計算
        return TriggerResult.FIRE;
    }

    /**
     * onEventTime：
     *   當 Event Time timer 觸發時執行
     *
     * 通常會用來處理 watermark 到達 window.end 時的觸發。
     *
     * 這裡回傳 CONTINUE 表示：
     *   → 不在事件時間觸發
     */
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        // 不基於事件時間觸發
        return TriggerResult.CONTINUE;
    }

    /**
     * clear：
     *   視窗結束、被刪除時會呼叫
     *   用來清除 timer 或 state
     *
     * 通常要呼叫：
     *   ctx.deleteEventTimeTimer(...)
     *   ctx.deleteProcessingTimeTimer(...)
     */
    @Override
    public void clear(TimeWindow window, TriggerContext ctx) {
        // Demo：沒用到 timer，就不需要做任何事
    }
}

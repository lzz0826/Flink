package org.utils;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class TimeUtil {

    /**
     * 範例輸入格式：
     *   "17/05/2015:12:05:45 +0000"
     *
     * 時間格式說明：
     *   dd/MM/yyyy:HH:mm:ss Z
     *   - dd：日期
     *   - MM：月份
     *   - yyyy：年份
     *   - HH:mm:ss：時分秒
     *   - Z：時區（例如 +0000 = UTC）
     *
     * @param timeStr Apache log 的時間字串
     * @return Unix timestamp（毫秒）
     */
    public static long parseTimeToTimestamp(String timeStr , String timeZone) {

        String time = timeStr + " " + timeZone;

        // 定義 Apache Log 時間格式（含時區 Z）
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
                "dd/MM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

        // 解析字串成 ZonedDateTime（會自動處理時區）
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(time, formatter);

        // 轉換成 Unix timestamp（毫秒）
        return zonedDateTime.toInstant().toEpochMilli();
    }

}

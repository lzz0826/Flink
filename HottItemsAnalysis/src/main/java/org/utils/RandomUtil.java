package org.utils;

import java.util.concurrent.ThreadLocalRandom;

public class RandomUtil {

    /**
     * 回傳 1 ~ max 之間的隨機整數（包含 max）
     *
     * @param max 最大值（例如 10）
     * @return 隨機整數，範圍為 1 ~ max
     */
    public static int randomInt(int max) {
        if (max <= 0) {
            throw new IllegalArgumentException("max 必須大於 0");
        }
        return ThreadLocalRandom.current().nextInt(1, max + 1);
    }

}

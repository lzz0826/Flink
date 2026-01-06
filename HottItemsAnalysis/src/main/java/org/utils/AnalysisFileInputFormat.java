package org.utils;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


//*** 實際開發要使用 Flink 提供的 自定義的會有很多沒有考慮的因數
public class AnalysisFileInputFormat extends FileInputFormat<String> {

    // transient：避免序列化，因為 BufferedReader 無法序列化
    private transient BufferedReader reader;

    public AnalysisFileInputFormat(Path filePath) {
        super(filePath);
    }

    /**
     * open 方法
     * 功能：
     *   - 每個 split 開始讀取前會呼叫
     *   - 初始化 inputStream 與 BufferedReader
     * 呼叫時機：
     *   - Flink 開始讀取該檔案區塊時
     */
    @Override
    public void open(FileInputSplit split) throws IOException {
        super.open(split);
        FSDataInputStream inputStream = this.stream;  // Flink 的輸入流
        reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));

        // **關鍵 解決多分區讀取問題：如果不是第一個分割點，跳過直到讀到第一個換行符，確保從行首開始
        if (split.getStart() != 0) {
            // 從當前位置開始讀取，直到讀到換行符為止（即跳過不完整的第一行）
            reader.readLine();
        }
    }




    /**
     * 判斷是否讀取完畢
     * Flink 會反覆呼叫 reachedEnd() 來判斷是否還有資料
     */
    @Override
    public boolean reachedEnd() throws IOException {
        return !reader.ready();
    }


    /**
     * 讀取下一筆資料（每次回傳一行）
     * reuse：Flink 給的可重用物件（本例中沒用）
     */
    @Override
    public String nextRecord(String reuse) throws IOException {
        return reader.readLine();
    }


    /**
     * close 方法
     * 功能：
     *   - 在該 source 讀取結束後被呼叫
     *   - 關閉資源避免記憶體泄漏
     */
    @Override
    public void close() throws IOException {
        super.close();
        if (reader != null) {
            reader.close();
        }
    }
}

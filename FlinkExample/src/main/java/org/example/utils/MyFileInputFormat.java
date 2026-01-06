package org.example.utils;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MyFileInputFormat extends FileInputFormat<String> {

    // transient：避免序列化，因為 BufferedReader 無法序列化
    private transient BufferedReader reader;

    public MyFileInputFormat(Path filePath) {
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

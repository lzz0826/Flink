
# Flink

## FlinkExample 模塊(基本用法)

## HottItemsAnalysis 模塊(業務邏輯)
### 展示內容:
#### 實時統計分析:
實時熱門商品統計<br>
實時熱門頁面流量統計<br>
實時訪問流量統計<br>
app市場推廣統計<br>
頁面廣告點擊量統計<br>

#### 業務流程.風險控制:
頁面廣告黑名單過濾<br>
惡意登入監控<br>
訂單支付失效監控<br>
支付實時對仗<br>

## 電商用戶行為分析
• 統計分析<br>
點擊、瀏覽
熱門商品、近期熱門商品、分類熱門商品、流量統計

• 偏好統計<br>
收藏、喜歡、評分、打標籤
用戶畫像、推薦列表（結合特徵工程和機器學習算法）

• 風險控制<br>
下訂單、支付、登錄<br>
刷單監控、訂單失效監控、惡意登錄（短時間內頻繁登錄失敗）監控
<br>
<br>
# ---------------- Flink  窗口函數  ---------------
• 推薦的設計模式：分層處理 (Separation of Concerns)<br>
• 將「時間開窗」與「業務邏輯」分開處理，可以極大地簡化開發、測試和維護工作。

#### Flink SQL 時間窗口函數 (Grouping Window)
#### 核心結構：TUMBLE(time_attr, interval)

| 關鍵部分     | 簡潔定義                  | 作用類比           |
| ------ | ----------------------------- | -------------------- |
| TUMBLE / HOP / SESSION    | 窗口類型與邊界定義。           | 類似 GROUP BY，但分組的鍵是時間，將流切分成塊。 |
| time_attr   | 時間屬性欄位。            | 類似 ROWTIME (事件時間) 或 PROCTIME() (處理時間)。必須是 Flink 已經聲明為時間屬性的欄位 (例如您的 ts)。 |
| interval   | 窗口長度/步長。                | 確定每個時間塊有多長，以及多久計算一次。 (例如 INTERVAL '1' HOUR) |

<br>

| 窗口類型     | Flink 函數                  | 定義格式            | 應用場景 |  
| ------ | ----------------------------- | ------------------- |------|
| 滾動窗口    | TUMBLE。           | "TUMBLE(ts, 長度)" |  週期性報告（例如小時 PV/UV）。窗口不重疊，緊密銜接。   |
| 跳動窗口    | HOP。           | "HOP(ts, 步長, 長度)"  |  實時監控與趨勢分析（例如每 5 分鐘計算過去 1 小時的 Top N）。窗口會重疊。   |
| 會話窗口    | SESSION。           |  "SESSION(ts, 間隔)"   |  計算用戶連續操作（Session）的總時長或聚合。窗口邊界由數據驅動。   |

• 窗口輔助函數 (Grouping Window Auxiliaries)<br>
這些函數用於在 SELECT 語句中提取窗口的邊界，必須與 GROUP BY 中的窗口函數類型一致。

| 關鍵部分     | 簡潔定義                   | 作用類比            | 
| ------ | --------------------------- | ------------------- |
| TUMBLE_START | 該窗口的起始時間 | 類似時間區塊的左邊界（包含）。 |
| TUMBLE_END | 該窗口的結束時間。 | 類似時間區塊的右邊界（不包含）。最常用於標記聚合結果的時間。 |

# ---------------- Mysql  窗口函數  ---------------
### 代碼路徑: Flink/HottItemsAnalysis/src/main/java/org/analysis/tableSQLAPI
ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
<br>
分析目標是**「在一個分組內進行計算，但計算結果要貼回原始數據的每一行」**時，窗口函數就是最優解。

| 關鍵部分     | 簡潔定義                   | 作用類比            | 
| ------ | --------------------------- | ------------------- |
| OVER | 定義函數運行的數據範圍 | 告訴 Flink 這是一個窗口函數，而不是普通聚合。 |
| PARTITION BY | 定義分組鍵 | 類似 GROUP BY 將數據切分成獨立的集合。排名在每個集合內獨立計算。 |
| ORDER BY | 定義排序規則 | 確定在每個分組內，序號分配的順序。 |
| ROW_NUMBER() | 實際的計數器 | 為排序後的行賦予連續且不重複的整數序號 (1, 2, 3...)。 |
<br>
<br>
# ----- DataStream API (Stream API) Table/SQL API ------
### 代碼路徑: Flink/HottItemsAnalysis/src/main/java/org/analysis/dataStreamAPI
### DataStream API 獨有功能列表:

• 定時器 (Timer Service)<br>
KeyedProcessFunction 中的 onTimer() 方法

• 手動狀態控制<br>
KeyedProcessFunction 或 Rich*Function 中，使用 getRuntimeContext().getState() 註冊 ValueState, ListState, MapState 等。

• 底層上下文訪問<br>
ProcessFunction 和 KeyedProcessFunction 中的 Context 對象。

• 自定義 Window 觸發器<br>
window().trigger() 方法，實現 Trigger 介面。

• 側邊輸出<br>
OutputTag 和 Context.output() 方法。

• CEP (複雜事件處理)<br>
專門的 Flink CEP 庫，接收 DataStream 作為輸入。

• 自定義數據源/接收器<br>
實現 SourceFunction 或 SinkFunction 介面。
<br>
<br>
# ------- CEP 從 一連串事件 中，找出 符合某種行為模式（Pattern） 的「複雜事件」。 ----------
### 代碼路徑: Flink/HottItemsAnalysis/src/main/java/org/analysis/CEP
#### 輸入 -> 規則 -> 輸出

CEP 適合用在「有順序、有時間限制、跨多事件」的業務行為判斷
「先 A、再 B、然後 C」，就該考慮 CEP。

「CEP 適合用在需要判斷多個事件之間的順序、次數與時間關係的場景，例如風控、漏斗分析、流程監控。
如果只是單純統計或只看上一筆事件，CEP 反而會過重，我會用 window 或 state。」


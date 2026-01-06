package org.monitor;

//訂單支付監視器

//需求:
//用戶下單後 應設置訂單失效時間 以提高用戶支付的意願 降低系統風險
//用戶下單後15分鐘後為支付 測輸出流監控信息

//思路:
//利用CEP庫進行事件流的模式匹配 並設定匹配的時間間隔
//也可以利用狀態編程 用process function 實現處理邏輯

public class OrderPaymentMonitor {
}

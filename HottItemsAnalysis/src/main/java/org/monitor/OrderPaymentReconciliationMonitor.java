package org.monitor;


//訂單支付對帳監視器

//需求:
//用戶下單並支付後 應查詢到帳信息 進行實時對帳
//如有不匹配的支付信息或是到帳信息 輸出提示信息

//思路:
//從兩條流中分別讀取訂單支付信息和到帳信息 合併處理
//用 connect 連接合併兩條流 用CoProcessFunction 做匹配處理

public class OrderPaymentReconciliationMonitor {
}

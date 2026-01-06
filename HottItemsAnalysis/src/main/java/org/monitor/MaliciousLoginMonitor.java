package org.monitor;


//惡意登入監視器

//需求:
//用戶在短時間內頻繁登入失敗 有惡意程序的可能
//同一用戶(可以是不同ip) 在2秒內連續兩次登入失敗 需要報警

//思路:
//將用戶的登入失敗行為存入 ListState 去訂訂時器2秒後刪除 查看 ListState中有幾次登入失敗
//更加精確的檢測 可使用 CEP 庫實現事件流的模式匹配

public class MaliciousLoginMonitor {
}

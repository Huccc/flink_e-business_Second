package com.atguigu.app;

import com.mysql.jdbc.util.LRUCache;

public class LRUCacheDemo {
    public static void main(String[] args) {
        // JDBC 的 LRUCache
        LRUCache<String, String> lruCache = new LRUCache<String, String>(100);
        lruCache.put("", "");
        lruCache.get("");
    }
}

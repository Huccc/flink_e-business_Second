package com.atguigu.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// 饿汉式可能会造成资源浪费
// 懒汉式可能会造成线程安全
public class ThreadPoolUtil {

    private static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil(ThreadPoolExecutor threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {

        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(
                            4,
                            100, 100,
                            TimeUnit.SECONDS,
                            // 线程队列，排队作用，先进先出
                            // 数据来了，先进队列，
                            // 当队列（一般值很大）满了才会创建新的线程
                            new LinkedBlockingDeque<>());
                }
            }
        }

        return threadPoolExecutor;
    }


}




















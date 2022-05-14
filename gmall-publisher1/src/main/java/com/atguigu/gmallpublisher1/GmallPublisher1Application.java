package com.atguigu.gmallpublisher1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 调用顺序
 * web页面 请求地址 -> controller -> service(ProductStatsService) -> service(ProductStatsServiceImpl) -> mapper -> clickhouse ->
 * clickhouse数据返回给 -> service -> controller -> 页面
 */

@SpringBootApplication
//@MapperScan(basePackages = "com.atguigu.gmallpublisher1.mapper")
public class GmallPublisher1Application {

    public static void main(String[] args) {
        SpringApplication.run(GmallPublisher1Application.class, args);
    }

}




























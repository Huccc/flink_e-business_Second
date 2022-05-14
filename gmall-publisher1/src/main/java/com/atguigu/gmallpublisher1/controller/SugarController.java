package com.atguigu.gmallpublisher1.controller;

import com.atguigu.gmallpublisher1.service.ProductStatsService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Map;

//@Controller    // 默认返回的是页面

@RestController
// 可以返回普通的对象
public class SugarController {

    @Autowired
    private ProductStatsService productStatsService;

    @RequestMapping("/api/sugar/gmv")
//    @ResponseBody     // 返回普通对象
    // 外部的请求
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {
        if (date == 0) {
            date = getToday();
        }

        // 查询clickhouse数据
        BigDecimal gmv = productStatsService.getGmv(date);

        // 封装json并返回结果
        return "{  " +
                "  \"status\": 0,  " +
                "  \"msg\": \"\",  " +
                "  \"data\": " + gmv + " " +
                "}";
    }

    @RequestMapping("/api/sugar/tm")
    public String getGmvByTm(@RequestParam(value = "date", defaultValue = "0") int date,
                             @RequestParam(value = "limit", defaultValue = "5") int limit) {

        // 处理日期问题
        if (date == 0) {
            date = getToday();
        }

        // 查询clickhouse获取品牌gmv数据
        Map gmvByTm = productStatsService.getGmvByTm(date, limit);

        // 封装json数据并返回    \\.
        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": { " +
                "    \"categories\": [\"" +
                StringUtils.join(gmvByTm.keySet(), "\",\"") +
                "\"], " +
                "    \"series\": [ " +
                "      { " +
                "        \"name\": \"商品品牌\", " +
                "        \"data\": [" +
                StringUtils.join(gmvByTm.values(), ",") +
                "] " +
                "      } " +
                "    ] " +
                "  } " +
                "}";

    }

    private int getToday() {
        long time = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return Integer.parseInt(sdf.format(time));
    }
}

























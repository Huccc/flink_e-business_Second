package com.atguigu.gmallpublisher1.service.impl;

import com.atguigu.gmallpublisher1.mapper.ProductStatsMapper;
import com.atguigu.gmallpublisher1.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ProductStatsServiceImpl implements ProductStatsService {

    @Autowired
    // 框架自动帮助构建 ProductStatsMapper 的实现类
    private ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGmv(int date) {
        return productStatsMapper.selectGmv(date);
    }

    @Override
    public Map getGmvByTm(int date, int limit) {

        // 查询clickhouse获取品牌gmv的数据
        List<Map> mapList = productStatsMapper.selectGmvByTm(date, limit);

        // 创建HashMap用于存放处理后的结果数据
        HashMap<String, BigDecimal> result = new HashMap<>();

        // 遍历maplist 提取元素将数据放入result
        for (Map map : mapList) {
            result.put((String) map.get("tm_name"), (BigDecimal) map.get("order_amount"));
        }

        // 返回结果
        return result;
    }
}




















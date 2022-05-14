package com.atguigu.gmallpublisher1.service;

import java.math.BigDecimal;
import java.util.Map;

public interface ProductStatsService {
    BigDecimal getGmv(int date);

    Map getGmvByTm(int date,int limit);
}

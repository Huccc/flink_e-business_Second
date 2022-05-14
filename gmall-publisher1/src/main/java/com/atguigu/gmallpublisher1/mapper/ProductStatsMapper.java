package com.atguigu.gmallpublisher1.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Mapper
public interface ProductStatsMapper {

    @Select("select sum(order_amount) from product_stats_210826 where toYYYYMMDD(stt)=#{date}")
    BigDecimal selectGmv(int date);


    @Select("select tm_name,sum(order_amount) order_amount from product_stats_210826 where toYYYYMMDD(stt)=#{date} group by tm_name order by order_amount desc limit #{limit}")

        //import org.springframework.web.bind.annotation.RequestParam;
//    List<Map> selectGmvByTm(@RequestParam("date") int date, @RequestParam("limit") int limit);
    List<Map> selectGmvByTm(@Param("date") int date, @Param("limit") int limit);


}




















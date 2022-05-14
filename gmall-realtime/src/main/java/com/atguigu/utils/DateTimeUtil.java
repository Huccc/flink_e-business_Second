package com.atguigu.utils;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * LocalDate  年月日
 * LocalTime  时分秒
 * LocalDateTime  年月日 时分秒
 */
public class DateTimeUtil {

    private final static DateTimeFormatter formator = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // todo 转换为 年月日 时分秒
    public static String toYMDhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return formator.format(localDateTime);
    }

    // todo 转换为 年月日
    public static String toYMD(Date date) {
        LocalDate localDate = LocalDate.ofEpochDay(System.currentTimeMillis());
        return formator.format(localDate);
    }

    // todo 转换为 时分秒
    public static String tohms(Date date) {
        LocalTime localTime = LocalTime.ofNanoOfDay(System.currentTimeMillis());
        return formator.format(localTime);
    }

    public static Long toTs(String YmDHms) {
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formator);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
}


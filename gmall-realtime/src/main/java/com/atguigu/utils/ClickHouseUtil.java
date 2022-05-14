package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {

    public static <T> SinkFunction<T> getClickHouseSink(String sql) {


        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        try {

                            // 通过反射的方式获取当前JavaBean的所有字段
                            Class<?> clz = t.getClass();

                            // 公共字段
                            clz.getFields();
                            // 所有字段
                            Field[] fields = clz.getDeclaredFields();

//                            clz.getDeclaredField("");

                            // 获取方法名称
//                            Method[] methods = clz.getMethods();
//                            for (int i = 0; i < methods.length; i++) {
//                                Method method = methods[i];
//                                Object invoke = method.invoke(t);
//                            }

                            // 遍历字段数组  数组没有增强for循环  只能使用fori
                            int flat = 0;
                            for (int i = 0; i < fields.length; i++) {
                                // 获取字段
                                Field field = fields[i];

                                // 设置该字段的访问权限
                                field.setAccessible(true);

                                // 获取字段注解
                                TransientSink transientSink = field.getAnnotation(TransientSink.class);
                                if (transientSink != null) {
                                    flat++;
                                    continue;
                                }

                                // 获取该字段对应的值信息
                                Object value = field.get(t);
                                // 给预编译sql 占位符赋值
                                preparedStatement.setObject(i + 1 - flat, value);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                },
                // 配置批量操作
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }
}




















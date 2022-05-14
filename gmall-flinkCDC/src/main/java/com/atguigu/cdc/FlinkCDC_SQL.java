package com.atguigu.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_SQL {
    public static void main(String[] args) throws Exception {
        // 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table mysql_binlog(" +
                "id int not null," +
                "name string " +
                ")with(" +
                "'connector'='mysql-cdc'," +
                "'hostname'='hadoop102'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='123456'," +
                "'database-name'='gmall_flink_0826'," +
                "'table-name'='base_sale_attr'" +
                ")");

        tableEnv.executeSql("select * from mysql_binlog")
                .print();

        env.execute();
    }
}

























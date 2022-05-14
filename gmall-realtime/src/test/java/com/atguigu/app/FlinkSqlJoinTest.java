package com.atguigu.app;

import com.atguigu.bean.Bean1;
import com.atguigu.bean.Bean2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class FlinkSqlJoinTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 打印默认的时间  PT0S  processing time 处理时间  0S 相当于状态清楚的功能关了
        System.out.println(tableEnv.getConfig().getIdleStateRetention());

        // TODO 帮助清理状态的参数
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        SingleOutputStreamOperator<Bean1> bean1DS = env.socketTextStream("hadoop102", 9999)
                .map(value -> {
                    String[] split = value.split(",");
                    return new Bean1(split[0], split[1], Long.parseLong(split[2]));
                });

        SingleOutputStreamOperator<Bean2> bean2DS = env.socketTextStream("hadoop103", 9999)
                .map(value -> {
                    String[] split = value.split(",");
                    return new Bean2(split[0], split[1], Long.parseLong(split[2]));
                });

        // 将流转换为动态表
        tableEnv.createTemporaryView("t1", bean1DS);
        tableEnv.createTemporaryView("t2", bean2DS);

        // 双流JOIN
        // 内连接    左：OnCreateAndWrite    右：OnCreateAndWrite
//        tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 join t2 on t1.id=t2.id")
//                .print();

        // 左外连接     左：OnReadAndWrite     右：OnCreateAndWrite
//        tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 left join t2 on t1.id=t2.id")
//                .print();

        // 全外连接      左：OnReadAndWrite       右：OnReadAndWrite
        tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 full join t2 on t1.id=t2.id")
                .print();

        env.execute();
    }
}



























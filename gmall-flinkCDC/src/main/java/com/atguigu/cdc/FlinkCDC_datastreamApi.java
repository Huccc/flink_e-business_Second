package com.atguigu.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC_datastreamApi {
    public static void main(String[] args) throws Exception {
        // 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

/*        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK
        env.enableCheckpointing(5000L);
        //2.2 指定CK的一致性语义(默认就是 EXACTLY_ONCE ,所以这块可以不取指定)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 指定从CK自动重启策略（现在也不用设置了，默认重启一次）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        //2.5 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
        //2.6 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");*/


        // 设置Mysql的相关属性
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_flink_0826")
                .tableList("gmall_flink_0826.base_sale_attr")
                // 程序启动时将以什么方式来获取到数据
                .startupOptions(StartupOptions.initial())
                // 读数据设置反序列化
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        // 将flinkCDC添加到source中
        env.addSource(sourceFunction).print();

        env.execute();
    }
}




















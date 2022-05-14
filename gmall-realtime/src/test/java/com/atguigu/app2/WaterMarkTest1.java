package com.atguigu.app2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WaterMarkTest1 {
    public static void main(String[] args) {
        // 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

    }
}

package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyDeseriali;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 生产环境中应该与Kafka的分区数保持一致  这样会导致效率最大化

    /*    //设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // 开启CK 以及 指定状态后端
        // 每5min做一次checkpoint
        env.enableCheckpointing(5 * 60000L);
        // 最多可以同时存在几个checkpoint
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 两个checkpoint之间最小间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // checkpoint的重启策略，现在默认一般是三次，不用管了
        env.getRestartStrategy();

        env.setStateBackend(new FsStateBackend(""));*/

        // TODO 2.读取Kafka  ods_base_db  主题的数据创建流
        DataStreamSource<String> KafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_db", "base_db_app_210826"));

        // TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> JsonObjDS = KafkaDS.map(JSON::parseObject);

        // TODO 4.过滤空数据（删除数据）
        // 获取主流
        SingleOutputStreamOperator<JSONObject> filterDS = JsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return !"delete".equals(value.getString("type"));
            }
        });

        // TODO 5.使用FlinkCDC读取配置表创建广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .password("123456")
                .username("root")
                .databaseList("gmall-210826-realtime")
                .tableList("gmall-210826-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeseriali())
                .build();

        DataStreamSource<String> flinkCDCDS = env.addSource(sourceFunction);

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        // 获取广播流
        BroadcastStream<String> broadcastStream = flinkCDCDS.broadcast(mapStateDescriptor);

        // TODO 6.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);

        // TODO 7.处理广播流数据和主流数据  分为Kafka和Hbase流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbaseTag") {
        };
        SingleOutputStreamOperator<JSONObject> kafkaMainDS = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        // TODO 8.提取两个流数据
        DataStream<JSONObject> hbaseDS = kafkaMainDS.getSideOutput(hbaseTag);

        // TODO 9.将两个流数据分别写出
        kafkaMainDS.print("Kafka>>>>>");
        hbaseDS.print("Hbase>>>>>>");

        // 写到hbase
        //Hbase>>>>>>> {"sinkTable":"dim_base_category1","database":"gmall_flink_0826","before":{},"after":{"name":"新增的2","id":19},"type":"insert","tableName":"base_category1"}
        hbaseDS.addSink(new DimSinkFunction());

        // 写到Kafka
        kafkaMainDS.addSink(MyKafkaUtil.getKafkaSink(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long timestamp) {
                return new ProducerRecord<>(
                        jsonObject.getString("sinkTable"),
                        jsonObject.getString("after").getBytes());
            }
        }));

        // TODO 10.启动任务
        env.execute("BaseDbApp");
    }
}




















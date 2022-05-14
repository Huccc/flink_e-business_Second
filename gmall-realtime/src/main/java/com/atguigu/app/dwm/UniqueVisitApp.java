package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

// 数据流:web/app -> Nginx -> 日志服务器 -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWM)
// 程序: Mock -> Nginx -> Logger.sh -> Kafka(ZK) -> BaseLogApp(分流的) -> Kafka(ZK) -> UniqueVisitApp -> Kafka(zk)

public class UniqueVisitApp {
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

        // TODO 2.消费Kafka   dwd_page_log    主题的数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

        DataStreamSource<String> KafksDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

        // TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> JsonObjDS = KafksDS.map(JSON::parseObject);

        // TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = JsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // TODO 5.使用状态编程对非今天访问的第一条数据做过滤
        // @FunctionalInterface 有这个注解才可以使用函数式编程
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> dsState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("dt_state", String.class);

                // 设置 ttl  状态过期时间  最多保存一天的日期
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                valueStateDescriptor.enableTimeToLive(stateTtlConfig);

                dsState = getRuntimeContext().getState(valueStateDescriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd");

            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                // 获取上一跳页面ID
                String lastpageid = value.getJSONObject("page").getString("last_page_id");
                if (lastpageid == null || lastpageid.equals("")) {
                    // 取出状态数据
                    String dt = dsState.value();

                    // 获取今天的日期
                    Long ts = value.getLong("ts");
                    String currentDt = sdf.format(ts);

                    if (dt == null || !dt.equals(currentDt)) {
                        // 更新状态并数据保留
                        dsState.update(currentDt);
                        return true;
                    }
                }

                return false;
            }
        });

        // TODO 6.将数据写出到Kafka
        filterDS.print();
        filterDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(sinkTopic));


//        filterDS.addSink(MyKafkaUtil.getKafkaSink(new KafkaSerializationSchema<JSONObject>() {
//            @Override
//            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
//                return new ProducerRecord<>(sinkTopic,);
//            }
//        }));


        // TODO 7.启动任务
        env.execute("UniqueVisitApp");
    }
}






























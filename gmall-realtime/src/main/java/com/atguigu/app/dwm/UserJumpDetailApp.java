package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

// 数据流:web/app -> Nginx -> 日志服务器 -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWM)
// 程序: Mock -> Nginx -> Logger.sh -> Kafka(ZK) -> BaseLogApp(分流的) -> Kafka(ZK) -> UserJumpDetailApp -> Kafka(zk)
public class UserJumpDetailApp {
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
        // TODO 2.读取Kafka页面主题的数据创建流
        String sourceTopic = "dwd_page_log";
        String groupId = "user_Jump_Detail_App_210826";
        String sinkTopic = "dwm_user_jump_detail";

        DataStreamSource<String> KafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

        // TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> JsonObjDS = KafkaDS.map(JSON::parseObject);

        // TODO 4.提取事件时间生成WaterMark
        SingleOutputStreamOperator<JSONObject> JsonObjWithWaterMarkDS = JsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        // TODO 5.按照mid进行分组
        KeyedStream<JSONObject, String> keyedStream = JsonObjWithWaterMarkDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // TODO 6.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("start")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .next("next")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .within(Time.seconds(10));

        // TODO 7.将模式序列作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // TODO 8.提取事件（匹配上的事件和超时事件）
        // 匹配的是要根据 WaterMark 来判断

        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("timeOut") {
        };

        SingleOutputStreamOperator<JSONObject> selectDS = patternStream
                .select(outputTag,
                        new PatternTimeoutFunction<JSONObject, JSONObject>() {
                            @Override
                            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                                return map.get("start").get(0);
                            }
                        },
                        new PatternSelectFunction<JSONObject, JSONObject>() {
                            @Override
                            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                                return map.get("start").get(0);
                            }
                        });

        // TODO 9.结合两个事件流
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(outputTag);
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        // TODO 10.将数据写入Kafka
        unionDS.print();
        unionDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        // TODO 11.启动任务
        env.execute("UserJumpDetailApp");
    }
}

















package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

// 数据流; web/app -> nginx -> 业务服务器 -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd)/Phoenix(dim) -> FlinkApp(Redis) -> Kafka(dwm) -> FlinkApp -> kafka(dwm)
// 程序: MockDb -> Mysql -> BaseDbApp -> Kafka(zk) -> BaseDbApp -> Kafka(zk)/phoenix(hbase,zk,hdfs) -> orderWideApp -> Kafka(zk)-> PaymentWideApp -> Kafka(zk)
public class PaymentWideApp {
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

        // TODO 2.消费 订单宽表和支付主题 的数据创建流
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        DataStreamSource<String> orderWideStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentInfoStrDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));

        // TODO 3.将每行数据转换为Javabean 并提起事件时间生成WaterMark
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                                try {
                                    long time = sdf.parse(element.getCreate_time()).getTime();
                                    return time;
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return System.currentTimeMillis();
                                }
                            }
                        }));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoStrDS.map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return System.currentTimeMillis();
                                }

                            }
                        }));

        // TODO 4.双流Join 注意延迟时间
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        // TODO 5.将数据写出到Kafka主题
        paymentWideDS.print("paymentWideDS>>>>>>>");
        paymentWideDS.map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        // TODO 6.启动任务
        env.execute("PaymentWideApp");
    }
}

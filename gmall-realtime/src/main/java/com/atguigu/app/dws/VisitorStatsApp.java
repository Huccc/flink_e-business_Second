package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

// 数据流: web/app -> Nginx -> 日志服务器 -> Kafka(ods) -> Flinkapp -> kafka(dwd) -> FlinkApp -> Kafka(dwm) -> flinkapp -> ClickHouse(dws)
// 程序: mock -> nginx -> Logger -> kafka(zk) -> baseLogapp -> Kafka(dwd) —> uv/uj -> kafka(dwm) -> visitorstatsapp -> clickhouse
public class VisitorStatsApp {
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

        // TODO 2.读取三个主题的数据创建流
        String groupId = "visitor_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uniqueVisitSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> uniqueVisitDStream = env.addSource(uniqueVisitSource);
        DataStreamSource<String> userJumpDStream = env.addSource(userJumpSource);

        // TODO 3.将每个流转换为相同的数据格式 JavaBean
        SingleOutputStreamOperator<VisitorStats> uvDS = uniqueVisitDStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObject.getLong("ts"));

        });


        SingleOutputStreamOperator<VisitorStats> ujDS = userJumpDStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObject.getLong("ts"));

        });

        SingleOutputStreamOperator<VisitorStats> pvDS = pageViewDStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            long sv = 0L;
            if (jsonObject.getJSONObject("page").getString("last_page_id") == null
//                    || jsonObject.getJSONObject("page").getString("last_page_id") == ""
            ) {
                sv = 1L;
            }

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    1L,
                    sv,
                    0L,
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        // TODO 4.合并三个流的数据
        DataStream<VisitorStats> unionDS = uvDS.union(ujDS, pvDS);

        // TODO 5.提取事件时间生成WaterMark
        // TODO 因为是事件时间 所以当事件时间为  1s时  在此开了一个[1，10）的窗口，最迟12S到  但是在uj处理时，within加上延迟  需要13秒才能来，超时了，进不了窗口了
        SingleOutputStreamOperator<VisitorStats> unionWithWaterMarkDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.
                <VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(14))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        // TODO 6.分组 开窗 聚合
        // 分组 开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowStream = unionWithWaterMarkDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getAr(), value.getCh(), value.getIs_new(), value.getVc());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // todo 增量聚合  数据少 算的快
//        windowStream.reduce(new ReduceFunction<VisitorStats>() {
//            @Override
//            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
//                return null;
//            }
//        });

        // todo 全量聚合  可以求前百分比  可以获取窗口信息
//        windowStream.apply(new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
//            @Override
//            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
//                window.getStart();
//                window.getEnd();
//            }
//        });

        SingleOutputStreamOperator<VisitorStats> result = windowStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                // 取出数据
                VisitorStats visitorStats = input.iterator().next();

                // 取出窗口信息
                String start = DateTimeUtil.toYMDhms(new Date(window.getStart()));
                String end = DateTimeUtil.toYMDhms(new Date(window.getEnd()));

                // 设置窗口信息
                visitorStats.setStt(start);
                visitorStats.setEdt(end);

                out.collect(visitorStats);
            }
        });


//        windowStream.process(new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
//            @Override
//            public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
//                TimeWindow window = context.window();
//
//            }
//        });


        // TODO 7.将数据写出到ClickHouse
        result.print("VisitorStatsApp>>>>>");
        result.addSink(ClickHouseUtil.getClickHouseSink("insert into visitor_stats_210826 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        // TODO 8.启动任务
        env.execute("VisitorStatsApp");
    }
}













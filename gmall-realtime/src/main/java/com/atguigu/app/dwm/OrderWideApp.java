package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.AsyncDimFunction;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
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

        // TODO 2.读取Kafka 订单和订单明细主题数据创建流
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";

        String groupId = "order_wide_group_210826";

        DataStreamSource<String> orderInfoStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId));

        DataStreamSource<String> orderDetailStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId));

        // TODO 3.将每行数据转换为JavaBean对象并提取时间戳生成WaterMark
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoStrDS.map(line -> {
                    OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);

                    // 处理数据中的时间  yyyy-MM-dd HH:mm:ss
                    String create_time = orderInfo.getCreate_time();

                    String[] dataTimeArr = create_time.split(" ");

                    orderInfo.setCreate_date(dataTimeArr[0]);
                    orderInfo.setCreate_hour(dataTimeArr[1].split(",")[0]);

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    // 获取时间戳
                    orderInfo.setCreate_ts(sdf.parse(create_time).getTime());

                    return orderInfo;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        }));

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailStrDS.map(line -> {
                    OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);

                    String create_time = orderDetail.getCreate_time();

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    orderDetail.setCreate_ts(sdf.parse(create_time).getTime());

                    return orderDetail;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderDetail>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        }));

        // TODO 4.双流 JOIN
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                // 给定公司中最大的延迟时间
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

//        orderWideDS.print("orderWideDS>>>>>");

        // TODO 5.关联维表
        // 同步的
//        orderWideDS.map(orderWide -> {
//
//            // 关联用户维度，查询用户维度
//
//            // 关联省份维度
//
//            //
//
//            return orderWide;
//        });

        // 采用异步方式
        // 5.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS,
                new AsyncDimFunction<OrderWide>("DIM_USER_INFO") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                        orderWide.setUser_gender(dimInfo.getString("GENDER"));

                        String birthday = dimInfo.getString("BIRTHDAY");
                        long ts = System.currentTimeMillis();

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long birthTs = sdf.parse(birthday).getTime();

                        long age = (ts - birthTs) / (1000L * 60 * 60 * 24 * 365);

                        orderWide.setUser_age((int) age);
                    }
                },
                60,
                TimeUnit.SECONDS);

//        orderWideWithUserDS.print("User>>>>>>");

        // 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS, new AsyncDimFunction<OrderWide>("DIM_BASE_PROVINCE") {
            @Override
            public String getKey(OrderWide input) {
                return input.getProvince_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                // 提取维度信息并设置进orderWide
                orderWide.setProvince_name(dimInfo.getString("NAME"));
                orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
            }
        }, 60, TimeUnit.SECONDS);

        //5.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new AsyncDimFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new AsyncDimFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.5 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new AsyncDimFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.6 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new AsyncDimFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print();

        // TODO 6.将数据写入Kafka主题
        orderWideWithCategory3DS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        // TODO 7.启动程序
        env.execute("OrderWideApp");
    }
}















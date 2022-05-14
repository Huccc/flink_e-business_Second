package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.AsyncDimFunction;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentWide;
import com.atguigu.bean.ProductStats;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Desc: 形成以商品为准的统计  曝光 点击  购物车  下单 支付  退单  评论数 宽表
 */

// 数据流:
    // web/app -> nginx -> 日志服务器 -> Kafka(ODS) -> FlinkApp -> Kafka(dwd)
    // web/app -> nginx -> 业务服务器 -> Mysql -> FlinkApp -> Kafka(ODS) -> FlinkApp -> Kafka(dwd) / Phoenix(dim) -> FlinkApp -> Kafka(dwm)
    //// =>> flinkapp -> clickhouse

// 程序
public class ProductStatsApp {
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

        // TODO 2.读取Kafka主题的数据创建流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        // TODO 3.将每个流转换为统一的JavaBean

        // 3.1 转换点击和曝光数据
        SingleOutputStreamOperator<ProductStats> productStatsWithPageViewDS = pageViewDStream.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {

                // 将数据转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                // 提取时间戳字段
                Long ts = jsonObject.getLong("ts");

                // 取出页面信息
                JSONObject page = jsonObject.getJSONObject("page");

                if ("good_detail".equals(page.getString("page_id")) && "sku_id".equals(page.getString("item_type"))) {
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build();
                    out.collect(productStats);
                }

                // 获取曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");

                if (displays != null && displays.size() > 0) {
                    // 遍历曝光数据
                    for (int i = 0; i < displays.size(); i++) {
                        // 获取每一条曝光数据
                        JSONObject display = displays.getJSONObject(i);

                        if ("sku_id".equals(display.getString("item_type"))) {

                            out.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        // 3.2 转换收藏数据
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorInfoDStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.3 转换加购数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartInfoDStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.4 转换订单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDStream.map(line -> {
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_amount(orderWide.getSplit_total_amount())
                    .order_sku_num(orderWide.getSku_num())
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        // 3.5 转换支付数据
        SingleOutputStreamOperator<ProductStats> productStatsWithPayDS = paymentWideDStream.map(line -> {
            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);

            HashSet<Long> orderIds = new HashSet<>();

            orderIds.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getSplit_total_amount())
                    .paidOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        // 3.6 转换退单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundInfoDStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            HashSet<Long> orderIds = new HashSet<>();

            orderIds.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.7 转换评价数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentInfoDStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            long goodCt = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(jsonObject.getString("appraise"))) {
                goodCt = 1L;

            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });


        // TODO 4.union 各个流
        DataStream<ProductStats> unionDS = productStatsWithPageViewDS
                .union(
                        productStatsWithFavorDS,
                        productStatsWithCartDS,
                        productStatsWithOrderDS,
                        productStatsWithPayDS,
                        productStatsWithRefundDS,
                        productStatsWithCommentDS);


        // TODO 5.提取事件时间生成WaterMark
        SingleOutputStreamOperator<ProductStats> unionWithWaterMarkDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        // TODO 6.分组 开窗 聚合
        WindowedStream<ProductStats, Long, TimeWindow> windowStream = unionWithWaterMarkDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<ProductStats> productStatsReduceDS = windowStream.reduce(new ReduceFunction<ProductStats>() {
            @Override
            public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                //stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                //stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                //stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                return stats1;

            }
        }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
            @Override
            public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {

                // 取出数据
                ProductStats productStats = input.iterator().next();

                // 设置窗口信息
                productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                // 补充三个订单次数指标
                productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                out.collect(productStats);
            }
        });

        // TODO 7.关联维度信息
        // 7.1 补充SKU 维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(productStatsReduceDS,
                new AsyncDimFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats input) {
                        return input.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setSku_name(dimInfo.getString("SKU_NAME"));
                        input.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        input.setSpu_id(dimInfo.getLong("SPU_ID"));
                        input.setTm_id(dimInfo.getLong("TM_ID"));
                        input.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        //7.2 补充SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new AsyncDimFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //7.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream =
                AsyncDataStream.unorderedWait(productStatsWithSpuDstream,
                        new AsyncDimFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //7.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream =
                AsyncDataStream.unorderedWait(productStatsWithCategory3Dstream,
                        new AsyncDimFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        // TODO 8.将数据写出到ClickHouse

        productStatsWithTmDstream.print("ProductStatsApp>>>>>>");

        productStatsWithTmDstream.addSink(ClickHouseUtil.getClickHouseSink("insert into product_stats_210826 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        // TODO 9.启动任务
        env.execute("ProductStatsApp");
    }
}
























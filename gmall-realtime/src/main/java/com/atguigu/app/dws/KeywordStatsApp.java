package com.atguigu.app.dws;

import com.atguigu.app.func.SplitFunction;
import com.atguigu.bean.KeywordStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
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

        // TODO 2.使用DDL方式读取Kafka主题数据创建动态表  注意：提取事件时间生成WaterMark
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";

//        tableEnv.sqlQuery("create table page_view(" +
//                "    common map<string,string>," +
//                "    page map<string,string>," +
//                "    ts bigint," +
//                "    rt as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
//                "    WATERMARK FOR rt as rt - INTERVAL '2' SECOND) " +
//                " WITH (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")");

        tableEnv.executeSql("CREATE TABLE page_view " +
                "(" +
                "common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>," +
                "ts BIGINT, " +
                "rt AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR rt AS rt - INTERVAL '2' SECOND" +
                ") " +
                "WITH (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")");

        // TODO 3.过滤数据
        // "page":{"during_time":2744,"item":"图书","item_type":"keyword","last_page_id":"search","page_id":"good_list"},

        Table fullWordTable = tableEnv.sqlQuery("select" +
                "    page ['item'] fullWord," +
                "    rt " +
                "from page_view " +
                "where" +
                "    page['item_type']='keyword' and page['item'] is not null");

        // TODO 4.注册UDTF函数，并炸裂搜索的关键词
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        tableEnv.createTemporaryView("full_word", fullWordTable);

        Table keyWordTable = tableEnv.sqlQuery("SELECT " +
                "    word, " +
                "    rt " +
                "FROM full_word, LATERAL TABLE(SplitFunction(fullWord)) ");

//        Table keyWordTable1 = tableEnv.sqlQuery("select word,rt from full_word ," +
//                " LATERAL TABLE(SplitFunction(fullWord))");

        // TODO 5.按照单词分组，计算wordcount

        tableEnv.createTemporaryView("word_table", keyWordTable);

        Table tableResult = tableEnv.sqlQuery("select  " +
                "    'search' source,  " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,  " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,  " +
                "    word keyword,  " +
                "    count(*) ct,  " +
                "    unix_timestamp()*1000 ts  " +
                "from word_table  " +
                "group by  " +
                "    word,  " +
                "    TUMBLE(rt,INTERVAL '10' SECOND)");

        // TODO 6.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(tableResult, KeywordStats.class);
        // TODO 7.将数据写出到clickhouse
        keywordStatsDS.print();
        keywordStatsDS.addSink(ClickHouseUtil.getClickHouseSink("" +
                // 按照Javabean的顺序  clickhouse表的列名来写
                "insert into keyword_stats_210826(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        // TODO 8.启动任务
        env.execute("KeywordStatsApp");
    }
}



























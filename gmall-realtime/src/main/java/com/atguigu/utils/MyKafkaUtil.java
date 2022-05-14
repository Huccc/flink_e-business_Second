package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MyKafkaUtil {

    private static String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static String DEFAULT_TOPIC = "DWD_DEFAULT_TOPIC";
    private static Properties properties = new Properties();

    static {
        properties.setProperty("bootstrap.servers", KAFKA_SERVER);
    }

    //TODO 将数据发送至Kafka
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

//    public static <T> FlinkKafkaProducer<T> getKafkaSink(String topic1) {
//        return new FlinkKafkaProducer<T>();
//    }


    // TODO 将数据发送至Kafka（使用默认主题）
    // TODO 泛型方法返回值前面要加<T>,声明
    public static <T> FlinkKafkaProducer<T> getKafkaSink(KafkaSerializationSchema<T> kafkaSerializationSchema) {

        // 方便理解
//        KafkaProducer kafkaProducer = new KafkaProducer<>();
//        kafkaProducer.send(ProducerRecord producerRecord);

        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,
                kafkaSerializationSchema,
                properties,
                // 当checkpoint不开时，下面都会使用默认值none
                FlinkKafkaProducer.Semantic.NONE);
    }

    //TODO 从Kafka获取数据
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<String>(topic,
                new SimpleStringSchema(),
                properties);
    }


    public static String getKafkaDDL(String topic, String groupId) {
        String ddl = "'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'  ";
        return ddl;
    }

}

























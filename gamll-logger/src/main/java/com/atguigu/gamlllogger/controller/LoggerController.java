package com.atguigu.gamlllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

//    Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test")
    public String test() {
        System.out.println("111111");
        return "success";
    }

    @RequestMapping("test1")
    public String test1(@RequestParam("name") String name,
                        @RequestParam("age") Integer age) {
        System.out.println("123");
        return "name: " + name + " -> " + "age: " + age;
    }

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr) {
//        System.out.println(jsonStr);

        // 将日志打印到控制台并落盘
        log.info(jsonStr);

        // 将数据发送到Kafka
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";
    }
}

















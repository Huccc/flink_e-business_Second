package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

// 接口里面都是抽象方法
public interface AsyncJoinFunction<T> {

    String getKey(T input);

    void join(T input, JSONObject dimInfo) throws ParseException;
}

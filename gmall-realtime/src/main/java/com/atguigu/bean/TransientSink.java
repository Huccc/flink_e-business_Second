package com.atguigu.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

// 当前注解作用在字段上
@Target(ElementType.FIELD)
// 什么时候生效   运行时生效
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {

}

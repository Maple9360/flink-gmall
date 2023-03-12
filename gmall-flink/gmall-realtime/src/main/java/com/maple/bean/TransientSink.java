package com.maple.bean;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)  // 范围
@Retention(RetentionPolicy.RUNTIME) // 运行时机
public @interface TransientSink {

}

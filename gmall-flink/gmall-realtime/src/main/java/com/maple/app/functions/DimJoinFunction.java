package com.maple.app.functions;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {

     String getKey(T t) ;
     void join(T t, JSONObject dimInfo);




}

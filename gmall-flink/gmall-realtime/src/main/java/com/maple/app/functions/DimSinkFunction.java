package com.maple.app.functions;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.maple.utils.DimUtil;
import com.maple.utils.DruidDSUtil;
import com.maple.utils.PhoenixUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class DimSinkFunction extends RichSinkFunction<JSONObject> implements SinkFunction<com.alibaba.fastjson.JSONObject> {

    private static DruidDataSource druidDataSource = null;


    @Override
    public void open(Configuration parameters) throws Exception {
       druidDataSource = DruidDSUtil.createDataSource();
    }



    @Override
    public void invoke(com.alibaba.fastjson.JSONObject value, Context context) throws Exception {
        // 获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();

        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");

        String type = value.getString("type");

        if ("update".equals(type)) {
            DimUtil.delDimInfo(sinkTable.toUpperCase(),data.getString("id"));
        }


        // 写出数据
        PhoenixUtil.updateValues(connection,sinkTable,data);// 可抛出异常，也可后续在表中查


        // 归还连接
        connection.close();
    }
}

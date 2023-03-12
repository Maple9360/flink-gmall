package com.maple.app.functions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maple.bean.TableProcess;
import com.maple.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    // s:>>>>> {"before":null,"after":{"source_table":"aa","sink_table":"bb","sink_columns":"cc","sink_pk":"id","sink_extend":"xxx"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1667289180892,"snapshot":"false","db":"gmall_config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1667289180902,"transaction":null}




    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    // 处理广播流
    @Override
    public void processBroadcastElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
        // 获取并解析数据
        JSONObject jsonObject = JSON.parseObject(s);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);


        // 校验并且建表
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());



        // 写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(),tableProcess);
    }


    /**
     * 校验并建表：create table if not exists db.tn(id varchar primary key,bb varchar,cc varchar) xxx
     * @param sinkTable
     * @param sinkColumns
     * @param sinkPk
     * @param sinkExtend
     */

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {


        PreparedStatement preparedStatement = null;


        try {
            // 处理字段

            if(sinkPk == null || "".equals(sinkPk)){
                sinkPk="id";
            }

            if(sinkExtend == null) {
                sinkExtend = "";
            }

            // 拼接sql

            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)   // 数据库
                    .append(".")
                    .append(sinkTable)  // 表
                    .append("(");


            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                // 取出字段
                String column = columns[i];

                if(sinkPk.equals(column)){
                    createTableSql.append(column).append(" varchar primary key");
                }else {
                    createTableSql.append(column).append(" varchar ");
                }

                if (i < columns.length - 1){
                    createTableSql.append(",");
                }

            }

            createTableSql.append(")").append(sinkExtend);

            // 编译sql
            System.out.println("建表语句为：" + createTableSql);

            preparedStatement = connection.prepareStatement(createTableSql.toString());


            // 执行sql
            preparedStatement.execute();


        } catch (SQLException throwables) {
            throw new RuntimeException("建表失败： " + sinkTable);
        } finally {


            if(preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }

        }


    }

    // 处理主流
    // value:{"database":"gmall","table":"base_trademark","type":"update","ts":1666781925,"xid":292,"commit":true,"data":{"id":12,"tm_name":"maple","logo_url":"/static/maple.jpg"},"old":{"logo_url":"/static/default.jpg"}}
    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {


        // 1.获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

        String table = jsonObject.getString("table");


        TableProcess tableProcess = broadcastState.get(table);


        // 2.过滤字段
        if (tableProcess != null){
            filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());

            // 3.补充SinkTable并写出到流中


            jsonObject.put("sinkTable",tableProcess.getSinkTable());

            collector.collect(jsonObject);


        }else{
            System.out.println("找不到对应的key：" + table);
        }


    }

    /**
     * 过滤字段
     * @param data  {"id":12,"tm_name":"maple","logo_url":"/static/maple.jpg"}
     * @param sinkColumns   id,tm_name
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        // 切分sinkColumns
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = data.entrySet();

//
//        while (iterator.hasNext()){
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columnList.contains(next.getKey())){
//                iterator.remove();
//            }
//        }

        entries.removeIf(next -> !columnList.contains(next.getKey()));
    }


    @Override
    public void close() throws Exception {
        connection.close();
    }
}

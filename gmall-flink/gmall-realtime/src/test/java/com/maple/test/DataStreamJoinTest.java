package com.maple.test;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DataStreamJoinTest {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(""+ "create TEMPORARY  table base_ dic( " +
                "`dic_ code`  String," +
                "`dic_ name`  String, " +
                "`parent_ code` string, " +
                "`create_ time`  String, " +
                "`operate_ time`  String " +
                ") WITH( " +
                "'connector' ='jdbc', " +
                "'url'= 'jdbc:mysql://hadoop102:3306/gmall', " +
                " 'driver' = 'com.mysql.cj.jdbc.Driver', "+
                "'table-name'= 'base_dic', " +
                "username' ='root'， " +
                "password'='000000' " +
                ")");

        tableEnv.sqlQuery("select * from base_dic").execute().print();




    }
}

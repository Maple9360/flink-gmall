package com.maple.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maple.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String key) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException {


        // 先查询redis
        // String key = key.trim();
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + key;

        String dimJsonStr = jedis.get(redisKey);




        if (dimJsonStr != null) {
            // 重置过期时间
            jedis.expire(redisKey,24 * 60 * 60);
            // 归还连接
            jedis.close();
            // 返回维度数据
            return JSON.parseObject(dimJsonStr);
        }


        // 拼接SQL语句
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + "  where id = '"+ key +"'";
        System.out.println("querySql>>>>" + querySql);

        // 查询数据
        List<JSONObject> jsonObjects = JDBCUtil.queryList(connection, querySql, JSONObject.class, false);


        // 将从Phoenix查询到的数据写入redis
        JSONObject dimInfo = jsonObjects.get(0);
        jedis.set(redisKey,dimInfo.toJSONString());

        //设置过期时间
        jedis.expire(redisKey,24 * 60 * 60);
        // 归还连接
        jedis.close();

        // 返回结果
        return dimInfo;
    }


    public static void delDimInfo(String tableName, String key) {

        // 获取连接
        Jedis jedis = JedisUtil.getJedis();

        // 删除连接
        jedis.del("DIM:" + tableName + ":" + key);

        // 归还连接
        jedis.close();

    }

    public static void main(String[] args) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        DruidPooledConnection connection = dataSource.getConnection();


        long start = System.currentTimeMillis();
        JSONObject dimInfo = getDimInfo(connection, "DIM_BASE_CATEGORY1", "2");
        long end = System.currentTimeMillis();
        JSONObject dimInfo2 = getDimInfo(connection, "DIM_BASE_CATEGORY1", "2");
        long end2 = System.currentTimeMillis();


        System.out.println(dimInfo);
        System.out.println(dimInfo2);

        System.out.println(end-start);
        System.out.println(end2-end);


    }





}

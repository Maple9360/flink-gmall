package com.maple.utils;




import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {
    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {

        // 创建集合用于存放结果数据 
        ArrayList<T> result = new ArrayList<>();

        // 编译sql语句
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        // 执行查询
        ResultSet resultSet = preparedStatement.executeQuery();


        // 获取查询的元数据信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();


        // 遍历结果集，将每行数据转换为T对象并加入集合
        while (resultSet.next()) {

            // 创建T对象
            T t = clz.newInstance();

            // 列遍历，并给对象赋值
            for (int i = 0; i < columnCount; i++) {
                // 获取列名与列值
                String columnName = metaData.getColumnName(i+1);
                System.out.println(columnName);
                Object value = resultSet.getObject(columnName);
                System.out.println(value);


                // 是否需要驼峰命名
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }

                // 赋值
                BeanUtils.setProperty(t,columnName,value);

            }

            result.add(t);
        }

        resultSet.close();
        preparedStatement.close();
        // 返回集合
        return result;
    }


    public static void main(String[] args) throws SQLException, IllegalAccessException, InvocationTargetException, InstantiationException {
        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        DruidPooledConnection connection = dataSource.getConnection();

        List<JSONObject> jsonObjects = queryList(connection,
                "select * from  GMALL_REALTIME.DIM_BASE_TRADEMARK",
                JSONObject.class,
                false);

        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject);
        }

        connection.close();

    }
}

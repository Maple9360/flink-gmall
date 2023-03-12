package com.maple.utils;

import com.maple.bean.TransientSink;
import com.maple.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil {
    public static <T> SinkFunction<T> getSinkFunction(String sql) {

        return JdbcSink.<T>sink(sql, new JdbcStatementBuilder<T>() {
            @SneakyThrows
            @Override
            public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                //  通过反射获取t对象中的数据
                Class<?> aClass = t.getClass();
                Field[] declaredFields = aClass.getDeclaredFields();
                int offset = 1;
                // 遍历属性
                for (int i = 0; i < declaredFields.length; i++) {
                    // 获取单个属性
                    Field field = declaredFields[i];
                    field.setAccessible(true);

                    // 获取注解
                    TransientSink transientSink = field.getAnnotation(TransientSink.class);
                    if(transientSink != null){
                        continue;
                    }

                    // 获取属性值
                    Object value = field.get(t);

                    // 给占位符赋值
                    preparedStatement.setObject(offset++,value);
                }
            }
        }, new JdbcExecutionOptions.Builder().
                        withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withPassword("123456")
                        .build());

    }
}

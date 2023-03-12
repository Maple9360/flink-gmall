package com.maple.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maple.app.functions.DimSinkFunction;
import com.maple.app.functions.TableProcessFunction;
import com.maple.bean.TableProcess;
import com.maple.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

// 数据流：web/app ->   nginx -> 业务服务器  ->  Mysql(binlog)  ->  Maxwell  ->  Kafka(ODS)   ->  FlinkApp   ->   Phoenix

// 程序：Mock  -> Mysql(binlog)   ->  Maxwell  ->  kafka(zk)  -> DimApp(FlinkCDC/Mysql)  ->  Phoenix(Hbase/Zk/HDFS)




public class DimApp {
    public static void main(String[] args) throws Exception {

        // 1.获取执行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 生产环境中设置为kafka主题的分区数
//          开启checkpoint
//        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE)
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
//         设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://master:8020/ck");
//        System.setProperty("HADOOP_USER_NAME","maple");

    // 2.读取Kafka topic_db 主题创建主流
        String topic = "topic_db";
        String groupId = "dim_app_log";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        kafkaDS.print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        // 3.过滤非JSON数据以及保留新增、变化、初始化的数据
        SingleOutputStreamOperator<JSONObject> jsonStreamOperator = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> collector) throws Exception {
                try {

                    JSONObject json = JSON.parseObject(value);
                    String type = json.getString("type");

                    if ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)) {
                        collector.collect(json);
                    }
                } catch (Exception e) {
                    System.out.println("发现脏数据：" + value);
                }
            }
        });

        // 4.使用FlinkCDC去读Mysql配置信息表创建配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("slave2")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");

        // 5.将配置流处理为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map_state",String.class,TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlSource.broadcast(mapStateDescriptor);

        // 6.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonStreamOperator.connect(broadcastStream);

        // 7.处理连接流，根据配置信息处理主流数据
        SingleOutputStreamOperator<JSONObject> dimDS = connectedStream.process(new TableProcessFunction(mapStateDescriptor));
        // 8.将数据写出到Phoenix
        dimDS.print(">>>>>>>>>>");

        dimDS.addSink(new DimSinkFunction());

        // 9.启动任务
        env.execute("DimApp");


    }
}

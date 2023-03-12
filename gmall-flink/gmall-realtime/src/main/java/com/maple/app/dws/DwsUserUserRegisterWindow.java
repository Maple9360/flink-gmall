package com.maple.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maple.bean.UserRegisterBean;
import com.maple.utils.DateFormatUtil;
import com.maple.utils.MyClickHouseUtil;
import com.maple.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.glassfish.jersey.internal.sonar.SonarJerseyCommon;

import java.time.Duration;


// 数据流：web/app -> nginx -> 业务服务器->  Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) FlinkApp -> ClickHouse(DWD)
// 程序：Mock -> Mysql ->   Maxwell -> Kafka(ZK) -> DwdUserRegister -> Kafka(ZK) -> DwsUserUserRegisterWindow -> ClickHouse(ZK)
public class DwsUserUserRegisterWindow {
    public static void main(String[] args) throws Exception {


        // 获取执行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取Kafka DWD层用户注册主题数据创建流
        String topic = "dwd_user_register";
        String groupId = "dws_user_user_register_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // 将每行数据转换为javaBean对象
        SingleOutputStreamOperator<UserRegisterBean> useRegisterDS = kafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            String createTime = jsonObject.getString("create_time");
            return new UserRegisterBean("", "", 1L, DateFormatUtil.toTs(createTime,true));
        });


        // 提取时间戳生成watermark
        SingleOutputStreamOperator<UserRegisterBean> watermark = useRegisterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
                    @Override
                    public long extractTimestamp(UserRegisterBean userRegisterBean, long l) {
                        return userRegisterBean.getTs();
                    }
                }));

        // 开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> resultDS = watermark.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean t1, UserRegisterBean t2) throws Exception {
                        t1.setRegisterCt(t1.getRegisterCt() + t2.getRegisterCt());
                        return t1;
                    }
                }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<UserRegisterBean> iterable, Collector<UserRegisterBean> collector) throws Exception {

                        UserRegisterBean next = iterable.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));

                        collector.collect(next);

                    }
                });
        // 将数据写到clickhouse
        resultDS.print(">>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_user_user_register_window values(?,?,?,?)"));

        // 启动任务
        env.execute("DwsUserUserRegisterWindow");

    }
}

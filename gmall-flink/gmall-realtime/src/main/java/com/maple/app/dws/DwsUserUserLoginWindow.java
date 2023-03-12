package com.maple.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maple.bean.UserLoginBean;
import com.maple.utils.DateFormatUtil;
import com.maple.utils.MyClickHouseUtil;
import com.maple.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

// 数据流：web/app -> nginx -> 业务服务器->  Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
// 程序：Mock(lg.sh) -> flume -> Kafka(ZK) -> baseLogApp -> Kafka(ZK) -> DwsUserUserLoginWindow -> ClickHouse(ZK)

public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取Kafka页面日志主题创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_user_userlogin_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // 转换数据为JSON对象并过滤数据
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);

                String uid = jsonObject.getJSONObject("common").getString("uid");
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                if (uid != null && ("login".equals(lastPageId) || lastPageId == null)) {
                    collector.collect(jsonObject);
                }
            }
        });


        // 提取事件时间生成watermark

        SingleOutputStreamOperator<JSONObject> watermark = jsonDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));


        // 按uid分组
        KeyedStream<JSONObject, String> keyedStream = watermark.keyBy(json -> json.getJSONObject("common").getString("uid"));


        // 使用状态编程获取独立用户以及七日回流用户
        SingleOutputStreamOperator<UserLoginBean> userDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {

            private ValueState<String> lastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastLoginState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_login_state", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<UserLoginBean> collector) throws Exception {

                String lastLoginDate = lastLoginState.value();
                Long ts = jsonObject.getLong("ts");
                String curDataDate = DateFormatUtil.toDate(ts);


                long uu = 0L;
                long back = 0L;

                if (lastLoginDate == null) {

                    uu = 1L;
                    lastLoginState.update(curDataDate);

                } else if (!lastLoginDate.equals(curDataDate)) {

                    uu = 1L;
                    if (DateFormatUtil.toTs(curDataDate) - DateFormatUtil.toTs(lastLoginDate) / (24 * 60 * 60 * 1000L) >= 8) {
                        back = 1L;
                    }
                    lastLoginState.update(curDataDate);

                }

                if (uu != 0L) {
                    collector.collect(new UserLoginBean("", "",
                            back, uu, ts));
                }

            }
        });


        // 开创聚合
        SingleOutputStreamOperator<UserLoginBean> resultDS = userDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean t1, UserLoginBean t2) throws Exception {
                        t1.setBackCt(t1.getBackCt() + t2.getBackCt());
                        t1.setUuCt(t1.getUuCt() + t2.getUuCt());
                        return t1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {

                        UserLoginBean next = iterable.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        collector.collect(next);
                    }
                });
        // 将数据写出到Clickhouse
        resultDS.print(">>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_user_user_login_window values(?,?,?,?,?)"));
        // 启动任务
        env.execute("DwsUserUserLoginWindow");
    }
}

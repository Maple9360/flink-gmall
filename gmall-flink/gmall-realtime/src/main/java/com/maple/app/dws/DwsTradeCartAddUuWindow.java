package com.maple.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maple.bean.CartAddUuBean;
import com.maple.utils.DateFormatUtil;
import com.maple.utils.MyClickHouseUtil;
import com.maple.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
// 数据流：web/app -> nginx -> 业务服务器（Mysql） -> Maxwell -> Kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> Clickhouse(DWS)
// 程序：Mock  ->  Mysql  ->  Maxwell  ->  Kafka(ZK)   -> DwdTradeCartAdd  -> Kafka(ZK) -> DwsTradeCartAddUuWindow  -> Clickhouse(ZK)
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取Kafka DWD层 加购事实表
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));


        // 将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(JSON::parseObject);
        jsonDS.print(">>>>>>>>>>>>");

        // 提取事件事件生成watermark
        SingleOutputStreamOperator<JSONObject> watermark = jsonDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {

                        String operate_time = jsonObject.getString("operate_time");
                        String create_time = jsonObject.getString("create_time");
                        if (operate_time != null) {
                            return DateFormatUtil.toTs(operate_time, true);
                        } else {
                            return DateFormatUtil.toTs(create_time);
                        }
                    }
                }));



        // 按照user_id分组
        KeyedStream<JSONObject, String> keyedStream = watermark.keyBy(json -> json.getString("user_id"));

        // 使用状态编程提取独立加购用户
        SingleOutputStreamOperator<CartAddUuBean> cartAddUuBeanDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {

            private ValueState<String> lastCartAddState;


            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> lastState = new ValueStateDescriptor<>("last_Cart_Add_State", String.class);


                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                lastState.enableTimeToLive(ttlConfig);

                lastCartAddState = getRuntimeContext().getState(lastState);
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<CartAddUuBean> collector) throws Exception {
                // 获取状态数据以及当前数据的日期

                String lastDataDate = lastCartAddState.value();

                String operate_time = jsonObject.getString("operate_time");
                String create_time = jsonObject.getString("create_time");

                String curDataDate = null;
                if(operate_time != null) {
                    curDataDate = operate_time.split(" ")[0];
                } else {
                    curDataDate = create_time.split(" ")[0];
                }


                if (lastDataDate != null || !lastDataDate.equals(curDataDate)) {
                    lastCartAddState.update(curDataDate);
                    collector.collect(new CartAddUuBean("", "", 1L, 0L));

                }

            }
        });


        // 开窗、聚合
        SingleOutputStreamOperator<CartAddUuBean> resultDS = cartAddUuBeanDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean t1, CartAddUuBean t2) throws Exception {
                        t1.setCartAddUuCt(t1.getCartAddUuCt() + t2.getCartAddUuCt());


                        return t1;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<CartAddUuBean> iterable, Collector<CartAddUuBean> collector) throws Exception {
                        CartAddUuBean next = iterable.iterator().next();

                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        next.setTs(System.currentTimeMillis());

                        collector.collect(next);


                    }
                });

        // 将数据写出到clickhouse
            resultDS.print(">>>>>");
            resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));
        // 启动任务

        env.execute("DwsTradeCartAddUuWindow");



    }
}

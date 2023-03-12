package com.maple.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maple.bean.TradeOrderBean;
import com.maple.utils.DateFormatUtil;
import com.maple.utils.MyClickHouseUtil;
import com.maple.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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

// 数据流：web/app -> nginx -> 业务服务器（Mysql） -> Maxwell -> Kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> kafka(DWD) -> FlinkApp -> Clickhouse(DWS)
// 程序：Mock  ->  Mysql  ->  Maxwell  ->  Kafka(ZK)   -> DwdTradeOrderPreProcess  -> Kafka(ZK)  -> DwdTradeOrderDetail  -> Kafka(ZK) -> DwsTradeOrderWindow  -> Clickhouse(ZK)

public class DwsTradeOrderWindow {
    public static void main(String[] args) throws Exception {

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 读取kafkaDWD层下单主题数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_order_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // 将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("value>>>>>>>>>>" + s);
                }

            }
        });




        // 按照order_detail_id分组
        KeyedStream<JSONObject, String> keyDS = jsonDS.keyBy(json -> json.getString("id"));

        // 按照order_detail_id进行去重
        SingleOutputStreamOperator<JSONObject> filterDS = keyDS.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("is_exists", String.class);

                stateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String state = valueState.value();

                if (state == null) {
                    valueState.update("1");
                    return true;
                } else {
                    return false;
                }

            }
        });

        // 提取事件时间生成watermark
        SingleOutputStreamOperator<JSONObject> watermark = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return DateFormatUtil.toTs(jsonObject.getString("create_time"), true);
                            }
                        })
        );

        // 按照user_id分组
        KeyedStream<JSONObject, String> keyedStream = watermark.keyBy(json -> json.getString("user_id"));

        // 提取独立下单用户
        SingleOutputStreamOperator<TradeOrderBean> tradeOrderBeanDS = keyedStream.map(new RichMapFunction<JSONObject, TradeOrderBean>() {

            private ValueState<String> lastOrderState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastOrderState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-order", String.class));
            }

            @Override
            public TradeOrderBean map(JSONObject jsonObject) throws Exception {

                String lastDate = lastOrderState.value();
                String curDate = jsonObject.getString("create_time").split(" ")[0];

                long orderUniqueUserCount = 0L;
                long orderNewUserCount = 0L;


                if (lastDate == null) {
                    orderUniqueUserCount = 1L;
                    orderNewUserCount = 1L;
                    lastOrderState.update(curDate);

                } else if (!lastDate.equals(curDate)) {
                    orderUniqueUserCount = 1L;
                    lastOrderState.update(curDate);
                }


                // 获取件数和单价
                Integer skuNum = jsonObject.getInteger("sku_num");
                Double orderPrice = jsonObject.getDouble("order_price");

                Double splitActivityAmount = jsonObject.getDouble("split_activity_amount");
                Double splitCouponAmount = jsonObject.getDouble("split_coupon_amount");




                return new TradeOrderBean("", "",
                        orderUniqueUserCount,
                        orderNewUserCount,
                        splitActivityAmount == null ? 0.0D : splitActivityAmount ,
                        splitCouponAmount == null ? 0.0D : splitCouponAmount ,
                        skuNum * orderPrice,
                        0L);

            }
        });

        // 开窗聚合
        SingleOutputStreamOperator<TradeOrderBean> resultDS = tradeOrderBeanDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean t1, TradeOrderBean t2) throws Exception {
                        t1.setOrderActivityReduceAmount(t1.getOrderActivityReduceAmount() + t2.getOrderActivityReduceAmount());
                        t1.setOrderCouponReduceAmount(t1.getOrderCouponReduceAmount() + t2.getOrderCouponReduceAmount());
                        t1.setOrderNewUserCount(t1.getOrderNewUserCount() + t2.getOrderNewUserCount());
                        t1.setOrderUniqueUserCount(t1.getOrderUniqueUserCount() + t2.getOrderUniqueUserCount());
                        t1.setOrderOriginalTotalAmount(t1.getOrderOriginalTotalAmount() + t2.getOrderOriginalTotalAmount());
                        return t1;
                    }
                }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TradeOrderBean> iterable, Collector<TradeOrderBean> collector) throws Exception {

                        TradeOrderBean next = iterable.iterator().next();

                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        next.setTs(System.currentTimeMillis());

                        collector.collect(next);

                    }
                });

        // 将数据写到Clickhouse
        resultDS.print(">>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"));


        // 执行环境
        env.execute("DwsTradeOrderWindow");


    }
}

package com.maple.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maple.bean.TradePaymentWindowBean;
import com.maple.utils.DateFormatUtil;
import com.maple.utils.MyClickHouseUtil;
import com.maple.utils.MyKafkaUtil;
import com.maple.utils.TimestampLtz3CompareUtil;
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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;


// 数据流：web/app -> nginx -> 业务服务器（Mysql） -> Maxwell -> Kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> kafka(DWD) -> FlinkApp -> kafka(DWD) -> FlinkApp -> ClickHouse(DWD)
// 程序：Mock  ->  Mysql  ->  Maxwell  ->  Kafka(ZK)   -> DwdTradeOrderPreProcess  -> Kafka(ZK)  -> DwdTradeOrderDetail  -> Kafka(ZK)  -> DwdTradePayDetailSuc  -> Kafka(ZK) -> DwsTradePaymentSucWindow  ->  ClickHouse(ZK)

public class DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取DWD层成功支付主题数据创建流
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_suc_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));


        // 将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = null;
                try {
                    jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });


        // 按照订单明细id分组
        KeyedStream<JSONObject, String> jsonKeyBy = jsonDS.keyBy(json -> json.getString("order_detail_id"));


        // 使用状态编程保留最新的数据输出
        SingleOutputStreamOperator<JSONObject> processDS = jsonKeyBy.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("value_state", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {


                JSONObject state = valueState.value();

                if (state == null) {
                    valueState.update(jsonObject);
                    context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 5000L);
                } else {

                    String stateRt = state.getString("row_op_ts");
                    String curRt = jsonObject.getString("row_op_ts");
                    int compare = TimestampLtz3CompareUtil.compare(stateRt, curRt);

                    if (compare != 1) {
                        valueState.update(jsonObject);
                    }
                }
            }


            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {

                // 输出并清空状态数据
                JSONObject value = valueState.value();
                out.collect(value);
                valueState.clear();

            }
        });

        // 提取事件事件生成watermark
        SingleOutputStreamOperator<JSONObject> watermark = processDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        String callbackTime = jsonObject.getString("callback_time");
                        return DateFormatUtil.toTs(callbackTime, true);
                    }
                }));

        // 按照user_id分组
        KeyedStream<JSONObject, String> keyedStream = watermark.keyBy(json -> json.getString("user_id"));


        // 提取独立支付成功用户数
        SingleOutputStreamOperator<TradePaymentWindowBean> tradePaymentWindowBean = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {


            private ValueState<String> lastDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-dt", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TradePaymentWindowBean> collector) throws Exception {
                String lastDate = lastDateState.value();
                String curDate = jsonObject.getString("callback_time").split(" ")[0];

                long payUniqueUserCount = 0L;
                long payNewUserCount = 0L;


                if (lastDate == null) {
                    payUniqueUserCount = 1L;
                    payNewUserCount = 1L;
                    lastDateState.update(curDate);

                } else if (!lastDate.equals(curDate)) {
                    payUniqueUserCount = 1L;
                    lastDateState.update(curDate);
                }


                if (payUniqueUserCount == 1L) {
                    collector.collect(new TradePaymentWindowBean("", "",
                            payUniqueUserCount,
                            payNewUserCount,
                            0L));
                }

            }
        });

        // 开窗聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> resultDS = tradePaymentWindowBean.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean t1, TradePaymentWindowBean t2) throws Exception {
                        t1.setPaymentSucUniqueUserCount(t1.getPaymentSucUniqueUserCount() + t2.getPaymentSucUniqueUserCount());
                        t1.setPaymentSucNewUserCount(t1.getPaymentSucNewUserCount() + t2.getPaymentSucNewUserCount());
                        return t1;
                    }
                }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TradePaymentWindowBean> iterable, Collector<TradePaymentWindowBean> collector) throws Exception {

                        TradePaymentWindowBean next = iterable.iterator().next();

                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        next.setTs(System.currentTimeMillis());

                        collector.collect(next);


                    }
                });

        // 将结果写到ClickHouse

        resultDS.print(">>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));
        // 执行
        env.execute("DwsTradePaymentSucWindow");


    }
}

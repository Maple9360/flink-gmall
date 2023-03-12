package com.maple.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maple.app.functions.DimAsyncFunction;
import com.maple.bean.TradeProvinceOrderBean;
import com.maple.utils.DateFormatUtil;
import com.maple.utils.MyClickHouseUtil;
import com.maple.utils.MyKafkaUtil;
import com.maple.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;


// 数据流：web/app -> nginx -> 业务服务器（Mysql） -> Maxwell -> Kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> kafka(DWD) -> FlinkApp -> Clickhouse(DWS)
// 程序：Mock  ->  Mysql  ->  Maxwell  ->  Kafka(ZK)   -> DwdTradeOrderPreProcess  -> Kafka(ZK)  -> DwdTradeOrderDetail  -> Kafka(ZK) -> DwsTradeProvinceOrderWindow(Phoenix(HBASE-HDFS、ZK)、redis)  -> Clickhouse(ZK)


public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 读取DWD层 Kafka 下单数据主题
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_province_order_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // 将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("Value>>>>>" + s);
                }
            }
        });

        // 按照订单明细ID分组、去重(取最后一条数据)
        KeyedStream<JSONObject, String> keyedStream = jsonDS.keyBy(json -> json.getString("id"));
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("value-state", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject lastValue = valueState.value();
                if (lastValue == null) {
                    valueState.update(jsonObject);
                    long l = context.timerService().currentProcessingTime();
                    context.timerService().registerProcessingTimeTimer(l + 5000L);
                } else {
                    // 取出状态数据以及当前数据中的时间字段
                    String lastTs = lastValue.getString("row_op_ts");
                    String curTs = jsonObject.getString("row_op_ts");

                    if (TimestampLtz3CompareUtil.compare(lastTs, curTs) != 1) {
                        valueState.update(jsonObject);
                    }

                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {

                // 输出数据并清空状态
                out.collect(valueState.value());
                valueState.clear();
            }
        });


        // 将每行数据转换为JavaBean
        SingleOutputStreamOperator<TradeProvinceOrderBean> tradeProvinceOrderBeanDS = filterDS.map(json -> {

            HashSet<String> orderIdSet = new HashSet<>();
            orderIdSet.add(json.getString("order_id"));


            return new TradeProvinceOrderBean("", "",
                    json.getString("province_id"),
                    "",
                    0L,
                    orderIdSet,
                    json.getDouble("split_total_amount)"),
                    DateFormatUtil.toTs(json.getString("create_time"), true));
        });

        tradeProvinceOrderBeanDS.print("tradeProvinceOrderBeanDS>>>>>>>");


        // 提取时间戳生成watermark
        SingleOutputStreamOperator<TradeProvinceOrderBean> watermark = tradeProvinceOrderBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderBean>() {
                            @Override
                            public long extractTimestamp(TradeProvinceOrderBean tradeProvinceOrderBean, long l) {
                                return tradeProvinceOrderBean.getTs();
                            }
                        }));

        // 分组开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = watermark.keyBy(bean -> bean.getProvinceId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean t1, TradeProvinceOrderBean t2) throws Exception {

                        t1.getOrderIdSet().addAll(t2.getOrderIdSet());
                        t1.setOrderAmount(t1.getOrderAmount() + t2.getOrderAmount());
                        return t1;
                    }
                }, new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<TradeProvinceOrderBean> iterable, Collector<TradeProvinceOrderBean> collector) throws Exception {
                        TradeProvinceOrderBean next = iterable.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setOrderCount((long) next.getOrderIdSet().size());
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));

                        collector.collect(next);
                    }
                });

        reduceDS.print("》》》》》》》》》》》》》");


        // 关联省份维表补充省份名称字段
        SingleOutputStreamOperator<TradeProvinceOrderBean> resultDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<TradeProvinceOrderBean>("DIM_BASE_PROVINCE") {

            @Override
            public String getKey(TradeProvinceOrderBean tradeProvinceOrderBean) {
                return tradeProvinceOrderBean.getProvinceId();
            }


            @Override
            public void join(TradeProvinceOrderBean tradeProvinceOrderBean, JSONObject dimInfo) {
                tradeProvinceOrderBean.setProvinceName(dimInfo.getString("NAME"));
            }


        }, 100, TimeUnit.SECONDS);


        // 将数据写到Clickhouse
        resultDS.print("resultDS>>>>>>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into  dws_trade_province_order_window values(?,?,?,?,?,?,?)"));

        // 启动任务
        env.execute("DwsTradeProvinceOrderWindow");

    }
}
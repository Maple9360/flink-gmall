package com.maple.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.maple.app.functions.DimAsyncFunction;
import com.maple.bean.TradeUserSpuOrderBean;
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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.mortbay.jetty.servlet.HashSessionIdManager;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;


// 数据流：web/app -> nginx -> 业务服务器（Mysql） -> Maxwell -> Kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> kafka(DWD) -> FlinkApp -> Clickhouse(DWS)
// 程序：Mock  ->  Mysql  ->  Maxwell  ->  Kafka(ZK)   -> DwdTradeOrderPreProcess  -> Kafka(ZK)  -> DwdTradeOrderDetail  -> Kafka(ZK) -> DwsTradeUserSpuOrderWindow(Phoenix(HBASE-HDFS、ZK)、redis)  -> Clickhouse(ZK)
public class DwsTradeUserSpuOrderWindow {
    public static void main(String[] args) throws Exception {

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 读取Kafka DWD层 下单主题数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_user_spu_order_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));


        // 转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("Value>>>>>" + s);
                }
            }
        });

        // 按照订单明细id分组
        KeyedStream<JSONObject, String> keyedStream = jsonDS.keyBy(json -> json.getString("id"));

        // 去重
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("is-exist", String.class);
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

        // 将数据据转换为javabean对象
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuOrderBeanDS = filterDS.map(json -> {

            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(json.getString("order_id"));


            return TradeUserSpuOrderBean.builder()
                    .skuId(json.getString("sku_id"))
                    .userId(json.getString("user_id"))
                    .orderAmount(json.getDouble("split_total_amount"))
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(json.getString("create_time"), true))
                    .build();
        });


        // 关联sku_info维表  补充spu_id,tm_id,category3_id

//        tradeUserSpuOrderBeanDS.map(new RichMapFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean>() {
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//
//            }
//
//            @Override
//            public TradeUserSpuOrderBean map(TradeUserSpuOrderBean tradeUserSpuOrderBean) throws Exception {
//                return null;
//            }
//        });

        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuOrderBeanAsyDS = AsyncDataStream.unorderedWait(
                tradeUserSpuOrderBeanDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                        System.out.println("tradeUserSpuOrderBean.getSkuId()"+tradeUserSpuOrderBean.getSkuId());
                        return tradeUserSpuOrderBean.getSkuId();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setSpuId(dimInfo.getString("SPU_ID"));
                        tradeUserSpuOrderBean.setTrademarkId(dimInfo.getString("TM_ID"));
                        tradeUserSpuOrderBean.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                    }


                },
                100,
                TimeUnit.SECONDS);
        tradeUserSpuOrderBeanAsyDS.print("tradeUserSpuOrderBeanAsyDS>>>>>");


        // 提取时间时间生成watermark
        SingleOutputStreamOperator<TradeUserSpuOrderBean> watermark = tradeUserSpuOrderBeanAsyDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeUserSpuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TradeUserSpuOrderBean>() {
                            @Override
                            public long extractTimestamp(TradeUserSpuOrderBean tradeUserSpuOrderBean, long l) {
                                return tradeUserSpuOrderBean.getTs();
                            }
                        }));

        // 分组开窗聚合
        KeyedStream<TradeUserSpuOrderBean, Tuple4<String, String, String, String>> tradeUserSpuOrderBeanKeyedStream = watermark.keyBy(new KeySelector<TradeUserSpuOrderBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) throws Exception {

                return new Tuple4<>(tradeUserSpuOrderBean.getUserId(),
                        tradeUserSpuOrderBean.getSpuId(),
                        tradeUserSpuOrderBean.getTrademarkId(),
                        tradeUserSpuOrderBean.getCategory3Id());
            }
        });
        SingleOutputStreamOperator<TradeUserSpuOrderBean> resultDS = tradeUserSpuOrderBeanKeyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradeUserSpuOrderBean>() {
                    @Override
                    public TradeUserSpuOrderBean reduce(TradeUserSpuOrderBean t1, TradeUserSpuOrderBean t2) throws Exception {

                        t1.getOrderIdSet().addAll(t2.getOrderIdSet());
                        t1.setOrderAmount(t1.getOrderAmount() + t2.getOrderAmount());
                        return t1;
                    }
                }, new WindowFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> key, TimeWindow timeWindow, Iterable<TradeUserSpuOrderBean> iterable, Collector<TradeUserSpuOrderBean> collector) throws Exception {
                        TradeUserSpuOrderBean next = iterable.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setOrderCount((long) next.getOrderIdSet().size());
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        collector.collect(next);

                    }
                });

        // 关联spu，tm，category为表补充相应的信息
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithSpuDS = AsyncDataStream.unorderedWait(resultDS, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SPU_INFO") {
            @Override
            public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                tradeUserSpuOrderBean.setSpuName(dimInfo.getString("SPU_NAME"));
            }

            @Override
            public String getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                return tradeUserSpuOrderBean.getSpuId();
            }
        }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithTm = AsyncDataStream.unorderedWait(reduceWithSpuDS, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_TRADEMARK") {
            @Override
            public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                tradeUserSpuOrderBean.setTrademarkName(dimInfo.getString("TM_NAME"));
            }

            @Override
            public String getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                return tradeUserSpuOrderBean.getTrademarkId();
            }
        }, 100, TimeUnit.SECONDS);


        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCa3= AsyncDataStream.unorderedWait(reduceWithTm, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY3") {
            @Override
            public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                tradeUserSpuOrderBean.setCategory3Name(dimInfo.getString("NAME"));
                tradeUserSpuOrderBean.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
            }

            @Override
            public String getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                return tradeUserSpuOrderBean.getCategory3Id();
            }
        }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCa2 = AsyncDataStream.unorderedWait(reduceWithCa3, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY2") {
            @Override
            public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {

                tradeUserSpuOrderBean.setCategory2Name(dimInfo.getString("NAME"));
                tradeUserSpuOrderBean.setCategory1Id(dimInfo.getString("CATEGORY1_ID").trim());
            }

            @Override
            public String getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                return tradeUserSpuOrderBean.getCategory2Id();
            }
        }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCa1 = AsyncDataStream.unorderedWait(reduceWithCa2, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY1") {
            @Override
            public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                tradeUserSpuOrderBean.setCategory1Name(dimInfo.getString("NAME"));

            }

            @Override
            public String getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) {
                return tradeUserSpuOrderBean.getCategory1Id();
            }
        }, 100, TimeUnit.SECONDS);



        // 将数据写出到clickhouse
        reduceWithCa1.print("reduceWithCa1>>>>>");
        reduceWithCa1.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        // 启动


        env.execute("DwsTradeUserSpuOrderWindow");


    }
}

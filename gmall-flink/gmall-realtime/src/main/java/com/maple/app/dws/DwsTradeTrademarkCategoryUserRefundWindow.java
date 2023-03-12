package com.maple.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maple.app.functions.DimAsyncFunction;
import com.maple.bean.TradeTrademarkCategoryUserRefundBean;
import com.maple.utils.DateFormatUtil;
import com.maple.utils.MyClickHouseUtil;
import com.maple.utils.MyKafkaUtil;
import com.mysql.cj.util.TimeUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;


// 数据流：web/app -> nginx -> 业务服务器（Mysql） -> Maxwell -> Kafka(ODS) -> FlinkApp -> kafka(DWD)-> FlinkApp -> ClickHouse(DWS)
// 程序：Mock  ->  Mysql  ->  Maxwell  ->  Kafka(ZK)   -> DwdTradeOrderRefund  -> Kafka(ZK) -> DwsTradeTrademarkCategoryUserRefundWindow(Phoenix(HBASE-HDFS、ZK)、redis)  -> ClickHouse(ZK)
public class DwsTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取kafka DWD层 退单主题数据
        String topic = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));


        kafkaDS.print("kafkaDS>>>>>>>>>");

        // 将每行数据转换为javabean
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> TradeTrademarkCategoryUserRefundBeanDS = kafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getString("order_id"));


            return TradeTrademarkCategoryUserRefundBean.builder()
                    .skuId(jsonObject.getString("sku_id"))
                    .userId(jsonObject.getString("user_id"))
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(jsonObject.getString("create_time"), true))
                    .build();
        });

        TradeTrademarkCategoryUserRefundBeanDS.print("TradeTrademarkCategoryUserRefundBeanDS>>>>");
        // 关联sku_info维表 补充tm_id以及category3_id
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> CategoryUserRefundBeanWithSku = AsyncDataStream.unorderedWait(TradeTrademarkCategoryUserRefundBeanDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                        return tradeTrademarkCategoryUserRefundBean.getSkuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject dimInfo) {
                        tradeTrademarkCategoryUserRefundBean.setTrademarkId(dimInfo.getString("TM_ID"));
                        tradeTrademarkCategoryUserRefundBean.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                    }


                }, 100, TimeUnit.SECONDS);

        // 分组开窗聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> windowReduceDS = CategoryUserRefundBeanWithSku.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public long extractTimestamp(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, long l) {
                                return tradeTrademarkCategoryUserRefundBean.getTs();
                            }
                        })).keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) throws Exception {
                return new Tuple3<>(tradeTrademarkCategoryUserRefundBean.getUserId(),
                        tradeTrademarkCategoryUserRefundBean.getTrademarkId(),
                        tradeTrademarkCategoryUserRefundBean.getCategory3Id());

            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean t1, TradeTrademarkCategoryUserRefundBean t2) throws Exception {

                        t1.getOrderIdSet().addAll(t2.getOrderIdSet());

                        return t1;
                    }
                }, new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple3<String, String, String> stringStringStringTuple3, TimeWindow timeWindow, Iterable<TradeTrademarkCategoryUserRefundBean> iterable, Collector<TradeTrademarkCategoryUserRefundBean> collector) throws Exception {

                        TradeTrademarkCategoryUserRefundBean next = iterable.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        next.setRefundCount((long) next.getOrderIdSet().size());

                        collector.collect(next);
                    }
                });


        // 关联维表补充其他字段
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withTrademarkDS = AsyncDataStream.unorderedWait(windowReduceDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_TRADEMARK") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                        return tradeTrademarkCategoryUserRefundBean.getTrademarkId();
                    }


                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject dimInfo) {
                        tradeTrademarkCategoryUserRefundBean.setTrademarkName(dimInfo.getString("TM_NAME"));
                    }

                }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withCATEGORY3DS = AsyncDataStream.unorderedWait(withTrademarkDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY3") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                        return tradeTrademarkCategoryUserRefundBean.getCategory3Id();
                    }


                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject dimInfo) {
                        tradeTrademarkCategoryUserRefundBean.setCategory3Name(dimInfo.getString("NAME"));
                        tradeTrademarkCategoryUserRefundBean.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                    }

                }, 100, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withCATEGORY2DS = AsyncDataStream.unorderedWait(withCATEGORY3DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY2") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                        return tradeTrademarkCategoryUserRefundBean.getCategory2Id();
                    }


                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject dimInfo) {
                        tradeTrademarkCategoryUserRefundBean.setCategory2Name(dimInfo.getString("NAME"));
                        tradeTrademarkCategoryUserRefundBean.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                    }

                }, 100, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withCATEGORY1DS = AsyncDataStream.unorderedWait(withCATEGORY2DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY1") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                        return tradeTrademarkCategoryUserRefundBean.getCategory1Id();
                    }


                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject dimInfo) {
                        tradeTrademarkCategoryUserRefundBean.setCategory1Name(dimInfo.getString("NAME"));
                    }

                }, 100, TimeUnit.SECONDS);
        // 将数据写到ClickHouse
        withCATEGORY1DS.print("withCATEGORY1DS>>>>>>>>");
        withCATEGORY1DS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        // 启动任务
        env.execute("DwsTradeTrademarkCategoryUserRefundWindow");


    }
}

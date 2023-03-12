package com.maple.app.dws;

import akka.japi.pf.FI;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maple.bean.TrafficPageViewBean;
import com.maple.utils.DateFormatUtil;
import com.maple.utils.MyClickHouseUtil;
import com.maple.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;



// 数据流：web/app -> nginx -> 日志服务器-> flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
// 数据流：web/app -> nginx -> 日志服务器-> flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
// 数据流：web/app -> nginx -> 日志服务器-> flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
//////==>Flink -> Clickhouse(DWS)

// 程序：Mock(lg.sh) -> flume -> Kafka(ZK) -> baseLogApp -> Kafka(ZK)
// 程序：Mock(lg.sh) -> flume -> Kafka(ZK) -> baseLogApp -> Kafka(ZK) -> DwdTrafficUniqueVisitorDetail -> Kafka(ZK)
// 程序：Mock(lg.sh) -> flume -> Kafka(ZK) -> baseLogApp -> Kafka(ZK) -> DwdTrafficUserJumpDetail -> Kafka(ZK)
//////==>DwsTrafficVcChArIsNewPageViewWindow -> ClickHouse(ZK)

public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取三个主题的数据创建流

        String uvTopic = "dwd_traffic_unique_visitor_detail";
        String ujdTopic = "dwd_traffic_user_jump_detail";
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_channel_page_view_window";
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(uvTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(ujdTopic, groupId));
        DataStreamSource<String> pageDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));


        // 统一数据格式
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));


        });

        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 0L, 1L,
                    jsonObject.getLong("ts"));


        });


        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewPageDS = pageDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");


            JSONObject page = jsonObject.getJSONObject("page");
            String last_page_id = page.getString("last_page_id");

            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, last_page_id == null ? 1L:0L, 1L, page.getLong("during_time"), 0L,
                    jsonObject.getLong("ts"));


        });


        // 将三个流进行Union
        DataStream<TrafficPageViewBean> unionDS = trafficPageViewUvDS.union(trafficPageViewUjDS, trafficPageViewPageDS);


        // 提取事件时间生成WaterMark
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWatermark = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(14))
                .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {

                        return trafficPageViewBean.getTs();
                    }
                }));

        // 分组开窗聚合
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowStream = trafficPageViewWatermark.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean trafficPageViewBean) throws Exception {
                return new Tuple4<>(trafficPageViewBean.getAr(), trafficPageViewBean.getCh(), trafficPageViewBean.getIsNew(), trafficPageViewBean.getVc());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));

//
//        // 增量聚合
//        windowStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
//            @Override
//            public TrafficPageViewBean reduce(TrafficPageViewBean trafficPageViewBean, TrafficPageViewBean t1) throws Exception {
//                return null;
//            }
//        });
//
//        // 全量聚合
//        windowStream.apply(new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
//            @Override
//            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
//
//            }
//        });


        SingleOutputStreamOperator<TrafficPageViewBean> resultDS = windowStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean t1, TrafficPageViewBean t2) throws Exception {
                t1.setSvCt(t1.getSvCt() + t2.getSvCt());
                t1.setUvCt(t1.getUvCt() + t2.getUvCt());
                t1.setUjCt(t1.getUjCt() + t2.getUjCt());
                t1.setPvCt(t1.getPvCt() + t2.getPvCt());
                t1.setDurSum(t1.getDurSum() + t2.getDurSum());
                return t1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                // 获取数据
                TrafficPageViewBean next = iterable.iterator().next();

                // 补充信息
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));


                // 修改TS
                next.setTs(System.currentTimeMillis());

                // 输出数据
                collector.collect(next);
            }
        });


        // 将数据写出到ClickHouse
        resultDS.print(">>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));
        env.execute("DwsTrafficVcChArIsNewPageViewWindow");
    }
}

package com.maple.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maple.bean.TrafficHomeDetailPageViewBean;
import com.maple.utils.DateFormatUtil;
import com.maple.utils.MyClickHouseUtil;
import com.maple.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
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


// 数据流：web/app -> nginx -> 业务服务器->  Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
// 程序：Mock(lg.sh) -> flume -> Kafka(ZK) -> baseLogApp -> Kafka(ZK) -> DwsTrafficPageViewWindow -> ClickHouse(ZK)
public class DwsTrafficPageViewWindow {
    public static void main(String[] args) throws Exception {

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 读取Kafka 页面日志主题数据创建流
        String topic = "dwd_traffic_page_log";
        String GroupId="dws_traffic_page_view_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, GroupId));



        // 将每行数据转换为JSON对象并过滤（首页与商品详情页）
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {

                JSONObject jsonObject = JSON.parseObject(s);
                // 获取当前页面id
                String pageId = jsonObject.getJSONObject("page").getString("page_id");

                if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                    collector.collect(jsonObject);
                }
            }
        });

        // 提取时间时间生成watermark

        SingleOutputStreamOperator<JSONObject> watermark = jsonObjectDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }));

        // 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = watermark.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // 使用状态编程过滤出首页与商品详情页的独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> trafficHomeDetailPageDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {

            private ValueState<String> homeLastState;
            private ValueState<String> detailLastState;


            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> homeState = new ValueStateDescriptor<>("home_last_state", String.class);
                ValueStateDescriptor<String> detailState = new ValueStateDescriptor<>("detail_last_state", String.class);

                // 设置TTL
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                homeState.enableTimeToLive(ttlConfig);
                detailState.enableTimeToLive(ttlConfig);


                homeLastState = getRuntimeContext().getState(homeState);
                detailLastState = getRuntimeContext().getState(detailState);
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {

                // 获取状态数据以及当前数据中的日期
                Long ts = jsonObject.getLong("ts");
                String curDataDate = DateFormatUtil.toDate(ts);
                String homeStateDate = homeLastState.value();
                String detailStateDate = detailLastState.value();


                // 定义当问首页或者详情页的数据
                long homeIni = 0L;
                long detailIni = 0L;


                // 如果状态为空或者状态时间与当前时间不同，则为需要的数据

                if ("home".equals(jsonObject.getJSONObject("page").getString("page_id"))) {
                    if (homeStateDate == null || !homeStateDate.equals(curDataDate)) {
                        homeIni = 1L;
                        homeLastState.update(curDataDate);
                    }
                } else {
                    if (detailStateDate == null || !detailStateDate.equals(curDataDate)) {
                        detailIni = 1L;
                        detailLastState.update(curDataDate);
                    }
                }


                // 满足任何一个数据不等于0，则写出
                if (homeIni == 1 || detailIni == 1) {
                    collector.collect(new TrafficHomeDetailPageViewBean("", "",
                            homeIni,
                            detailIni,
                            ts));
                }
            }
        });

        // 开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultDS = trafficHomeDetailPageDS
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean t1, TrafficHomeDetailPageViewBean t2) throws Exception {

                        t1.setHomeUvCt(t1.getHomeUvCt() + t2.getHomeUvCt());
                        t1.setGoodDetailUvCt(t1.getGoodDetailUvCt() + t2.getGoodDetailUvCt());
                        return t1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        // 获取数据
                        TrafficHomeDetailPageViewBean next = iterable.iterator().next();

                        // 补充数据
                        next.setTs(System.currentTimeMillis());
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));

                        // 输出数据
                        collector.collect(next);


                    }
                });

        // 将数据写出到ClickHouse
        resultDS.print(">>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));

        // 启动任务

        env.execute("DwsTrafficPageViewWindow");



    }
}

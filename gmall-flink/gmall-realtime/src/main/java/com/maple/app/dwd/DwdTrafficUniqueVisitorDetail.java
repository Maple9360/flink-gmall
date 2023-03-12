package com.maple.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maple.utils.DateFormatUtil;
import com.maple.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 数据流：web/app -> nginx -> 日志服务器-> flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
// 程序：Mock(lg.sh) -> flume -> Kafka(ZK) -> baseLogApp -> Kafka(ZK) -> DwdTrafficUniqueVisitorDetail -> Kafka(ZK)
public class DwdTrafficUniqueVisitorDetail {
    // 当天首次登录
    public static void main(String[] args) throws Exception {

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取Kafka页面  日志主题创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "unique_visitor_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));



        // 过滤掉上一跳页面不为null的数据并将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null) {
                        collector.collect(jsonObject);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(s);
                }
            }
        });

        // 按照Mid分组
        KeyedStream<JSONObject, String> keyDS = jsonDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        // 使用状态编程实现按照mid的去重
        SingleOutputStreamOperator<JSONObject> uvDS = keyDS.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> lastVisitState;// 状态生命周期

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last_visit", String.class);


                // 历史登录可以清楚，所以需要带有效期
                // 为了不让状态失效，而导致一天的数据记录多次，状态发生变化的同时，也更新有效时间
                StateTtlConfig build = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(build);

                lastVisitState = getRuntimeContext().getState(stateDescriptor);

            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {

                // 获取状态数据和当前时间戳转换为日期
                String value = lastVisitState.value();
                Long ts = jsonObject.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);

                if (value == null || !curDate.equals(value)) {
                    lastVisitState.update(curDate);
                    return true;
                } else {
                    return false;
                }
            }
        });

        // 将数据写到Kafka
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        uvDS.print(">>>>>>>");
        uvDS.map(json->json.toJSONString()).addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));

        // 启动
        env.execute("DwdTrafficUniqueVisitorDetail");
    }
}

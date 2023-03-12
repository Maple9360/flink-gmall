package com.maple.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maple.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


// 数据流：web/app -> nginx -> 日志服务器-> flume -> Kafka(ODS)  -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
// 程序：Mock(lg.sh) -> flume -> Kafka(ZK) -> baseLogApp -> Kafka(ZK) -> DwdTrafficUserJumpDetail -> Kafka(ZK)
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取Kafka 页面日志主题数据创建流
        String topic = "dwd_traffic_page_log";
        String GroupId = "user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, GroupId));



        // 将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(s -> JSON.parseObject(s));

        // 按照MID分组  watermark 跟key没有关系

        KeyedStream<JSONObject, String> keyDS = jsonDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))    // 乱序两秒，延迟两秒
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }))
                .keyBy(json -> json.getJSONObject("common").getString("mid"));

    // 定义CEP模式序列        未掌握
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));

//        Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject jsonObject) throws Exception {
//                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
//            }
//        }).times(2).consecutive().within(Time.seconds(10));



        // 将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyDS, pattern);

        // 提取事件(匹配上的事件以及超时事件)
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOUt") {
        };
        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, String>() {
            @Override
            public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0).toJSONString();
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            @Override
            public String select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0).toJSONString();
            }
        });

        DataStream<String> timeOutDS = selectDS.getSideOutput(timeOutTag);

        // 合并两种事件
        DataStream<String> unionDS = selectDS.union(timeOutDS);
        selectDS.print("select >>>>>>>>>>");
        timeOutDS.print("timeOut >>>>>>>>>>");
        String targetToic = "dwd_traffic_user_jump_detail";
        // 将数据写出到Kafka

        unionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(targetToic));

        // 启动
        env.execute("DwdTrafficUserJumpDetail");
    }
}

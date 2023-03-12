package com.maple.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.maple.utils.DateFormatUtil;
import com.maple.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// 数据流：web/app -> nginx -> 日志服务器-> flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
// 程序：Mock(lg.sh) -> flume -> Kafka(ZK) -> baseLogApp -> Kafka(ZK)
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 消费Kafka Topic_log 主题的数据创建流
        String topic = "topic_log";
        String group = "base_log_app";


        DataStreamSource<String> KafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, group));

        // 3. 过滤掉非JSON格式的数据&将每行数据转换为JSON对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty"){};

        SingleOutputStreamOperator<JSONObject> jsonDS = KafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject); // 是json，输出到主流
                } catch (Exception e) {

                    context.output(dirtyTag, s); // 不是json，输出到侧输出流

                }
            }
        });
        DataStream<String> dirtyDS = jsonDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty>>>>>>>>>>>>>");


        // 4. 按照Mid分组
        KeyedStream<JSONObject, String> keyDS = jsonDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        // 5. 使用状态编程做新老访客标记校验
        SingleOutputStreamOperator<JSONObject> newDS = keyDS.map(new RichMapFunction<JSONObject, JSONObject>() {


            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_visit", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {

                String is_new = jsonObject.getJSONObject("common").getString("is_new");
                Long ts = jsonObject.getLong("ts");
                String cur_date = DateFormatUtil.toDate(ts);

                String lastDate = lastVisitState.value();

                if ("1".equals(is_new)) {

                    if (lastDate == null) {
                        lastVisitState.update(cur_date);
                    } else if (!cur_date.equals(lastDate)) {
                        jsonObject.getJSONObject("common").put("is_new", "0");
                    }

                } else if (lastDate == null) {
                    lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                }

                return jsonObject;
            }
        });
        // 6. 使用测输出流进行分流处理  页面日志放到主流  其他分别放在错误、启动、曝光、动作。
        OutputTag<String> actionTag = new OutputTag<String>("actionTag"){};
        OutputTag<String> errorTag = new OutputTag<String>("errorTag"){};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag"){};
        OutputTag<String> startTag = new OutputTag<String>("startTag"){};

        SingleOutputStreamOperator<String> pageDS = newDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {

                // 尝试获取错误信息
                String err = jsonObject.getString("err");
                if (err != null) {
                    // 将数据写入错误输出流
                    context.output(errorTag, jsonObject.toJSONString());
                }

                // // 尝试获取启动信息
                String start = jsonObject.getString("start");
                if (start != null) {
                    // 将数据写入启动输出流
                    context.output(startTag, jsonObject.toJSONString());
                } else {

                    // 不是启动，就是页面，页面有曝光和动作,同时获取公共信息。
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    JSONArray actions = jsonObject.getJSONArray("actions");

                    // 公共信息
                    JSONObject common = jsonObject.getJSONObject("common");
                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                    String ts = jsonObject.getString("ts");

                    // 尝试获取曝光信息
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            context.output(displayTag, display.toJSONString());
                        }
                    }

                    // 尝试获取动作信息
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page_id", pageId);
                            context.output(actionTag, action.toJSONString());
                        }
                    }


                    jsonObject.remove("displays");
                    jsonObject.remove("actions");

                    collector.collect(jsonObject.toJSONString());

                }
            }
        });


        // 7. 提取各个侧输出流数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);


        // 8. 将数据打印并写入对应的主题
        pageDS.print("Page>>>>>>>>>>>>>>");
        startDS.print("Start>>>>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>>>>>>");
        actionDS.print("Action>>>>>>>>>>>>>>");
        errorDS.print("Error>>>>>>>>>>>>>>") ;

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));



        // 9. 执行
        env.execute("BaseLogApp");
    }
}

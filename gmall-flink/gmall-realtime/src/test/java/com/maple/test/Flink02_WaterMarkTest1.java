package com.maple.test;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Flink02_WaterMarkTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //1001,23.6,1324
        DataStreamSource<String> socketTextStream = env.socketTextStream("master", 9999);
        SingleOutputStreamOperator<String> andWatermarks = socketTextStream.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                String[] split = element.split(",");
                return new Long(split[2]) * 1000L;
            }
        }));

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = andWatermarks.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0],
                    Double.parseDouble(split[1]),
                    Long.parseLong(split[2]));
        });

        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(WaterSensor::getId);

        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("vc")
                .print(">>>>>>>>");

        env.execute();

    }

}

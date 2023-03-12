package com.maple.utils;

import com.sun.istack.internal.Nullable;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;





public class MyKafkaUtil {


    private static final String KAFKA_SERVER = "master:9092";
//    private static final String ZOOKEEPER_HOST="master:2181,slave1:2181,slave2:2181/kafka2";


    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic,String groupId){
        Properties properties = new Properties();
//        properties.setProperty("zookeeper.connect",ZOOKEEPER_HOST);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        return new FlinkKafkaConsumer<String>(
                topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {

                        if(consumerRecord == null || consumerRecord.value() == null){
                            return "";
                        }else{
                            return new String(consumerRecord.value());
                        }

                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                },
                properties);
    }


    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(KAFKA_SERVER,topic,new SimpleStringSchema());

    }

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic, String defaultTopic) {

        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60 * 15 * 1000 + "");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(defaultTopic, new KafkaSerializationSchema<String>() {

            @Override
            public ProducerRecord<byte[], byte[]> serialize(String jsonStr, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(topic, jsonStr.getBytes());
            }
        }, prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return producer;
    }


    /**
     * Kafka-Source DDL 语句
     *
     * @param topic   数据源主题
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     */
    public static String getKafkaDDL(String topic, String groupId) {

        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'group-offsets')";
    }


    /**
     *
     * @param groupId
     * @return
     */

    public static String getTopicDB(String groupId) {
        return "CREATE TABLE topic_db(   " +
                "  `database` STRING,   " +
                "  `table` STRING,   " +
                "  `type` STRING,   " +
                "  `data`  MAP<STRING,STRING>,   " +
                "  `old`    MAP<STRING,STRING>,   " +
                " `pt` AS PROCTIME()   " +
                ") " + getKafkaDDL("topic_db",groupId);
    }




    /**
     * Kafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 Kafka-Sink DDL 语句
     */
    public static String getKafkaSinkDDL(String topic) {
        return "WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                "  'format' = 'json' " +
                ")";
    }

    /**
     * UpsertKafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 UpsertKafka-Sink DDL 语句
     */
    public static String getUpsertKafkaDDL(String topic) {

        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }
}




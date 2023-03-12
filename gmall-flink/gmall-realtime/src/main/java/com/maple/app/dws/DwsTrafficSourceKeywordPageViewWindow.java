package com.maple.app.dws;

import com.maple.app.functions.KeywordUDTF;
import com.maple.bean.KeywordBean;
import com.maple.utils.MyClickHouseUtil;
import com.maple.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import javax.smartcardio.CardTerminal;
// 数据流：web/app -> nginx -> 业务服务器->  Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
// 程序：Mock(lg.sh) -> flume -> Kafka(ZK) -> baseLogApp -> Kafka(ZK) -> DwsTrafficSourceKeywordPageViewWindow -> ClickHouse(ZK)
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 使用DDL方式读取Kafka page_log主题的数据创建表并且提取时间戳生成watermark
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window";
        tableEnv.executeSql("" +
                "        create table page_log(  " +
                "                `page` map<string,string>,  " +
                "                `ts` bigint,  " +
                "                `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),  " +
                "                WATERMARK FOR rt as rt - INTERVAL '2' SECOND  " +
                "        )" + MyKafkaUtil.getKafkaDDL(topic, groupId));


        // 过滤出搜索数据
        Table filterTable = tableEnv.sqlQuery("" +
                "        select page['item'] item,  " +
                "                rt  " +
                "        from page_log  " +
                "        where page['last_page_id'] = 'search'   " +
                "        and page['item_type'] = 'keyword'   " +
                "        and page['item'] is not null");

        tableEnv.createTemporaryView("filter_table",filterTable);


        // 注册UDTF & 分词
        tableEnv.createTemporarySystemFunction("SplitFunction",KeywordUDTF.class);
        Table splitTable = tableEnv.sqlQuery("" +
                "     select  " +
                "                word,  " +
                "                rt  " +
                "     from filter_table,  " +
                "     lateral table(SplitFunction(item))");

        tableEnv.createTemporaryView("split_table",splitTable);

        // 分组、开窗、聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "        select date_format(tumble_start(rt,interval '10' second),'yyyy-MM-dd HH:mm:ss')stt,  " +
                "                date_format(tumble_end(rt,interval '10' second),'yyyy-MM-dd HH:mm:ss')edt,  " +
                "                'search' source,  " +
                "                word  keyword," +
                "                count(*) keyword_count,  " +
                "                unix_timestamp() * 1000 ts  " +
                "        from split_table  " +
                "        group by word,TUMBLE(rt, INTERVAL '10' SECOND)");

        // 将动态表转换为流
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);
        keywordBeanDataStream.print(">>>>>");

        // 将数据写出到Clickhouse
        keywordBeanDataStream.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));

        // 启动任务
        env.execute("DwsTrafficSourceKeywordPageViewWindow");

    }
}

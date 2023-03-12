package com.maple.app.dwd;

import com.maple.utils.MyKafkaUtil;
import com.maple.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


// 数据流：web/app -> nginx -> 业务服务器（Mysql） -> Maxwell -> Kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> kafka(DWD)  FlinkApp -> kafka(DWD)
// 程序：Mock  ->  Mysql  ->  Maxwell  ->  Kafka(ZK)   -> DwdTradeOrderPreProcess  -> Kafka(ZK)  -> DwdTradeOrderDetail  -> Kafka(ZK)  -> DwdTradePayDetailSuc  -> Kafka(ZK)

public class DwdTradePayDetailSuc {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取TopicDB数据并过滤出支付成功数据
        tableEnv.executeSql(MyKafkaUtil.getTopicDB("pay_detail_suc"));
        
        Table paymentInfo = tableEnv.sqlQuery("select    " +
                "data['user_id'] user_id,    " +
                "data['order_id'] order_id,    " +
                "data['payment_type'] payment_type,    " +
                "data['callback_time'] callback_time,    " +
                "`pt`    " +
                "from topic_db    " +
                "where `table` = 'payment_info'    "
                +
                "and `type` = 'update'    " +
                "and data['payment_status']='1602'"
        );
        tableEnv.createTemporaryView("payment_info", paymentInfo);
        tableEnv.toAppendStream(paymentInfo, Row.class).print(">>>>>>");


        // 消费下单主题数据
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail(  " +
                "id string,  " +
                "order_id string,  " +
                "user_id string,  " +
                "sku_id string,  " +
                "sku_name string,  " +
                "sku_num string,  " +
                "order_price string,  " +
                "province_id string,  " +
                "activity_id string,  " +
                "activity_rule_id string,  " +
                "coupon_id string,  " +
                "create_time string,  " +
                "source_id string,  " +
                "source_type_id string,  " +
                "source_type_name string,  " +
                //"split_original_amount string,  " +
                "split_activity_amount string,  " +
                "split_coupon_amount string,  " +
                "split_total_amount string, " +
                //"ts string,  " +
                "row_op_ts timestamp_ltz(3)  " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_detail","pay_detail_suc_order"));




        // 读取Mysql Base_dic 表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // 三表关联
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "od.id order_detail_id, " +
                "od.order_id, " +
                "od.user_id, " +
                "od.sku_id, " +
                "od.sku_name, " +
                "od.province_id, " +
                "od.activity_id, " +
                "od.activity_rule_id, " +
                "od.coupon_id, " +
                "pi.payment_type payment_type_code, " +
                "dic.dic_name payment_type_name, " +
                "pi.callback_time, " +
                "od.source_id, " +
                "od.source_type_id, " +
                "od.source_type_name, " +
                "od.sku_num, " +
                "od.order_price,  " +
                //"od.split_original_amount, " +
                "od.split_activity_amount, " +
                "od.split_coupon_amount, " +
                "od.split_total_amount split_payment_amount, " +
                //"pi.ts, " +
                "od.row_op_ts row_op_ts " +
                "from payment_info pi " +
                "join dwd_trade_order_detail od " +
                "on pi.order_id = od.order_id " +
                "join `base_dic` for system_time as of pi.pt as dic " +
                "on pi.payment_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);




        // 创建kafka 支付成功表
        tableEnv.executeSql("create table dwd_trade_pay_detail_suc(  " +
                "order_detail_id string,  " +
                "order_id string,  " +
                "user_id string,  " +
                "sku_id string,  " +
                "sku_name string,  " +
                "province_id string,  " +
                "activity_id string,  " +
                "activity_rule_id string,  " +
                "coupon_id string,  " +
                "payment_type_code string,  " +
                "payment_type_name string,  " +
                "callback_time string,  " +
                "source_id string,  " +
                "source_type_id string,  " +
                "source_type_name string,  " +
                "sku_num string,  " +
                "order_price string,  " +
                //"split_original_amount string,  " +
                "split_activity_amount string,  " +
                "split_coupon_amount string,  " +
                "split_payment_amount string,  " +
                //"ts string,  " +
                "row_op_ts timestamp_ltz(3),  " +
                "primary key(order_detail_id) not enforced  " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_detail_suc"));



        // 将数据写出
        tableEnv.executeSql("insert into dwd_trade_pay_detail_suc select * from result_table");



        // 启动任务
        // env.execute();
        // 验证失败

    }
}

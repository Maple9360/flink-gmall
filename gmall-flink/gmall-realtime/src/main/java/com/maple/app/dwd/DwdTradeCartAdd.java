package com.maple.app.dwd;

import com.maple.utils.MyKafkaUtil;
import com.maple.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


// 数据流：web/app -> nginx -> 业务服务器（Mysql） -> Maxwell -> Kafka(ODS) -> FlinkApp -> kafka(DWD)
// 程序：Mock  ->  Mysql  ->  Maxwell  ->  Kafka(ZK)   -> DwdTradeCartAdd  -> Kafka(ZK)
public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 使用DDL方式读取topic_db 主题的数据创建表

        tableEnv.executeSql(MyKafkaUtil.getTopicDB("cart_add"));

        // 过滤出加购数据

        Table cartAddTable = tableEnv.sqlQuery("select  " +
                "  `data`['id']  id,  " +
                "  `data`['user_id']  user_id,  " +
                "  `data`['sku_id']  sku_id,  " +
                "  `data`['cart_price']  cart_price,  " +
                "  if(type='insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as String))  sku_num,  " +
                "  `data`['sku_name']  sku_name,  " +
                "  `data`['is_checked']  is_checked,  " +
                "  `data`['create_time']  create_time,  " +
                "  `data`['operate_time']  operate_time,  " +
                "  `data`['is_ordered']  is_ordered,  " +
                "  `data`['order_time']  order_time,  " +
                "  `data`['source_type']  source_type,  " +
                "  `data`['source_id']  source_id,  " +
                "  pt   " +
                "from topic_db  " +
                "where `database`='gmall' and `table` = 'cart_info' and (type= 'insert'   " +
                "      or (`type`='update'   " +
                "          and   " +
                "          `old`['sku_num'] is not null   " +
                "          and   " +
                "          cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int) ) " +
                ") ");

        // tableEnv.toAppendStream(cartAddTable, Row.class).print(">>>>>>");
            tableEnv.createTemporaryView("cart_info_table",cartAddTable);

        // 读取Mysql的base_dic 表作为LookUp表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        
        // 关联两张表
        Table cartAddWithDicTable = tableEnv.sqlQuery("" +
                "        select  " +
                "                ci.id,  " +
                "                ci.user_id,  " +
                "                ci.sku_id,  " +
                "                ci.cart_price,  " +
                "                ci.sku_num,  " +
                "                ci.sku_name,  " +
                "                ci.is_checked,  " +
                "                ci.create_time,  " +
                "                ci.operate_time,  " +
                "                ci.is_ordered,  " +
                "                ci.order_time,  " +
                "                ci.source_type source_type_id,  " +
                "                dic.dic_name source_type_name,  " +
                "                ci.source_id  " +
                "        from cart_info_table ci  " +
                "        join base_dic for system_time as of ci.pt dic  " +
                "        on ci.source_type=dic.dic_code");


        tableEnv.createTemporaryView("cart_add_dic_table",cartAddWithDicTable);
        // 使用DDL方式创建架构事实表
        tableEnv.executeSql("" +
                "create table dwd_cart_add(   " +
                "                `id` String,   " +
                "                `user_id` String,   " +
                "                `sku_id` String,   " +
                "                `cart_price` String,   " +
                "                `sku_num` String,   " +
                "                `sku_name` String,   " +
                "                `is_checked` String,   " +
                "                `create_time` String,   " +
                "                `operate_time` String,   " +
                "                `is_ordered` String,   " +
                "                `order_time` String,   " +
                "                `source_type_id` String,   " +
                "                `source_type_name` String,   " +
                "                `source_id` String   " +
                "        )" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));



        // 将数据写出
        // tableEnv.executeSql("insert into dwd_cart_add select * from " + cartAddWithDicTable);
        tableEnv.executeSql("insert into dwd_cart_add select * from cart_add_dic_table").print();
     // 启动任务
        env.execute("DwdTradeCartAdd");


    }
}

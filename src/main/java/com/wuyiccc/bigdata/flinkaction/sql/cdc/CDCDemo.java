package com.wuyiccc.bigdata.flinkaction.sql.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class CDCDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String query ="CREATE TABLE orders(" +
                "id BIGINT," +
                "user_id BIGINT," +
                "create_time TIMESTAMP(0)," +
                "payment_way STRING," +
                "delivery_address STRING," +
                "order_status STRING," +
                "total_amount DECIMAL(10, 5)) WITH (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = '127.0.0.1'," +
                "'port' = '3306'," +
                "'username' = 'long'," +
                "'password' = 'long'," +

                "'database-name' = 'flink'," +
                "'table-name' = 'orders')";

        tEnv.executeSql(query);
        String query2 = "SELECT * FROM orders";
        Table result2=tEnv.sqlQuery(query2);
        tEnv.toRetractStream(result2, Row.class).print();
        env.execute("CDC Job");
    }

}

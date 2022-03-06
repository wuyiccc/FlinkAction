package com.wuyiccc.bigdata.flinkaction.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;


public class DescribeDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        //注册表"Orders"
        tableEnv.executeSql(
                "CREATE TABLE Orders (" +
                        " `user` BIGINT NOT NULl," +
                        " product VARCHAR(32)," +
                        " amount INT," +
                        " ts TIMESTAMP(3)," +
                        " ptime AS PROCTIME()," +
                        " PRIMARY KEY(`user`) NOT ENFORCED," +
                        " WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS" +
                        ") ");
        // 打印schema
        tableEnv.executeSql("DESCRIBE Orders").print();
    }
}

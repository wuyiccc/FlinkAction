package com.wuyiccc.bigdata.flinkaction.demo;

import com.wuyiccc.bigdata.utils.source.MySource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author wuyiccc
 * @date 2021/12/9 22:22
 */
public class SQLStreamDemo {

    public static void main(String[] args) throws Exception {
        // 构建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建数据源
        DataStreamSource<String> stream = env.addSource(new MySource());
        Table table = tEnv.fromDataStream(stream, $("word"));


        // 查询数据
        Table result = tEnv.sqlQuery("SELECT * FROM " + table + " WHERE word LIKE '%t%'");


        // 表转化为流打印
        tEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }
}

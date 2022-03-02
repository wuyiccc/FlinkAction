package com.wuyiccc.bigdata.flinkaction.demo;

import com.wuyiccc.bigdata.flinkaction.demo.utils.source.MySource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author wuyiccc
 * @date 2021/12/8 23:14
 */
public class WordCountTableForStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 定义初始化表参数
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                // 设置blink为必需的模块
                .useBlinkPlanner()
                // 设置组件在流模式下工作, 默认启用
                .inStreamingMode()
                .build();

        // 创建Table API, SQL程序的执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, settings);

        /********/

        DataStreamSource<String> myStreamSource = sEnv.addSource(new MySource());
        // 将数据集转化为表
        Table table1 = tEnv.fromDataStream(myStreamSource, $("word"));

        // 对表进行操作
        Table table = table1.where($("word").like("%t%"));


        String explanationOld = tEnv.explain(table);
        System.out.println(explanationOld);

        // 将表转化为流数据
        tEnv.toAppendStream(table, Row.class).print();
        sEnv.execute();
    }
}

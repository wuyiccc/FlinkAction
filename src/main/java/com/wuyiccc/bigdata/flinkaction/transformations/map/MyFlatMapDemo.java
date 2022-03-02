package com.wuyiccc.bigdata.flinkaction.transformations.map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

import javax.sql.DataSource;

/**
 * @author wuyiccc
 * @date 2022/3/3 0:48
 * 使用FlatMap算子拆分句子
 */
public class MyFlatMapDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSource = env.fromElements("Apache Flink is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams");
        FlatMapOperator<String, String> flatMap = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String word : value.split(" ")) {
                    out.collect(word);
                }
            }
        });
        flatMap.print();
    }
}

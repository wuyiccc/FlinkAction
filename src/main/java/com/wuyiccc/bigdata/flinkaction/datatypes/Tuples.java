package com.wuyiccc.bigdata.flinkaction.datatypes;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * @author wuyiccc
 * @date 2022/3/2 23:52
 * 在Flink中使用元组类Tuple
 */
public class Tuples {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple1<String>> dataSource = env.fromElements(Tuple1.of("BMW"), Tuple1.of("Tesla"), Tuple1.of("Rolls-Roycle"));

        DataSet<String> ds = dataSource.map((MapFunction<Tuple1<String>, String>) value -> "I love " + value.f0);
        ds.print();
    }
}

package com.wuyiccc.bigdata.flinkaction.transformations.project;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author wuyiccc
 * @date 2022/3/3 0:56
 */
public class MyProjectDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, Double, String>> inPut = env.fromElements(
                new Tuple3<>(1, 2.0, "BWM"),
                new Tuple3<>(2, 2.4, "Tesla")
        );
        // 投送第三个和第二个字段
        DataSet<Tuple2<String, Integer>> out = inPut.project(2, 1);
        out.print();
    }
}

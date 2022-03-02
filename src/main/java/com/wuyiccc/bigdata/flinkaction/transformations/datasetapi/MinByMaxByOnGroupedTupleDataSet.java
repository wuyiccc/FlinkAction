package com.wuyiccc.bigdata.flinkaction.transformations.datasetapi;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author wuyiccc
 * @date 2022/3/3 7:46
 * 在分组元组上进行比较运算
 */
public class MinByMaxByOnGroupedTupleDataSet {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, String, Double>> input = env.fromElements(
                new Tuple3<>(1, "a", 1.0),
                new Tuple3<>(2, "b", 2.0),
                new Tuple3<>(4, "b", 4.0),
                new Tuple3<>(2, "b", 1.0),
                new Tuple3<>(3, "c", 3.0)
        );
        DataSet<Tuple3<Integer, String, Double>> output1 = input.groupBy(1)
                // 选择每个分组中组合字段(0, 2)中最小的数据
                .minBy(0, 2);
        output1.print();
    }

}

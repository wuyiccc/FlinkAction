package com.wuyiccc.bigdata.flinkaction.transformations.datasetapi;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author wuyiccc
 * @date 2022/3/3 7:35
 */
public class AggregateOnGroupedTupleDataSet {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, String, Double>> input = env.fromElements(
                new Tuple3<>(1, "a", 1.0),
                new Tuple3<>(2, "b", 2.0),
                new Tuple3<>(4, "b", 4.0),
                new Tuple3<>(3, "c", 3.0)
        );

        // 对数据集应用多个聚合
        DataSet<Tuple3<Integer, String, Double>> output1 = input.groupBy(1)
                // 产生字段1的总和
                .aggregate(Aggregations.SUM, 0)
                // 产生字段3的最小值的数据集
                .and(Aggregations.MIN, 2);

        // 在第一次聚合的基础上再次应用聚合
        DataSet<Tuple3<Integer, String, Double>> output2 = input.groupBy(1)
                .aggregate(Aggregations.SUM, 0)
                .aggregate(Aggregations.MIN, 2);
        output1.print();
        output2.print();
    }
}

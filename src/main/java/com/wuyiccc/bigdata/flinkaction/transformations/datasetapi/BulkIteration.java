package com.wuyiccc.bigdata.flinkaction.transformations.datasetapi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;

/**
 * @author wuyiccc
 * @date 2022/3/4 0:09
 * 全量迭代
 */
public class BulkIteration {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        IterativeDataSet<Integer> initial = env.fromElements(0).iterate(10);
        MapOperator<Integer, Integer> iteration = initial.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                double x = Math.random();
                double y = Math.random();
                return value + ((x * x + y * y < 1) ? 1 : 0);
            }
        });
        DataSet<Integer> count = initial.closeWith(iteration);
        count.map(new MapFunction<Integer, Double>() {
            @Override
            public Double map(Integer value) throws Exception {
               return value / ((double) 10) * 4;
            }
        }).print();
    }
}

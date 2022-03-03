package com.wuyiccc.bigdata.flinkaction.transformations.datasetapi.demo14;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author wuyiccc
 * @date 2022/3/3 20:36
 * 使用键选择器对元组数据进行分组与聚合
 */
public class ReduceDataSetGroupedByKeyFieldSelector {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<String, Integer, Double>> tuples = env.fromElements(
                new Tuple3<>("BMW", 30, 2.0),
                new Tuple3<>("Tesla", 30, 2.0),
                new Tuple3<>("Tesla", 10, 2.0),
                new Tuple3<>("Rolls-Roycle", 300, 4.0)
        );
        DataSet<Tuple3<String, Integer, Double>> reducedTuples = tuples.groupBy(0, 1)
                .reduce(new MyTupleReducer());
        reducedTuples.print();
    }

    public static class MyTupleReducer implements ReduceFunction<Tuple3<String, Integer, Double>> {

        @Override
        public Tuple3<String, Integer, Double> reduce(Tuple3<String, Integer, Double> value1, Tuple3<String, Integer, Double> value2) throws Exception {
            return new Tuple3<>(value1.f0, value1.f1 + value2.f1, value1.f2);
        }
    }
}

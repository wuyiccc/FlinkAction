package com.wuyiccc.bigdata.flinkaction.dataset.semanticannotation.noforwardedField;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.expressions.In;

/**
 * @author wuyiccc
 * @date 2022/3/4 7:36
 * 声明非转发字段
 */
public class NonForwardedFieldDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer, Integer>> input = env.fromElements(
                Tuple2.of(1, 2)
        );
        input.map(new MyMap()).print();
    }

    @FunctionAnnotation.NonForwardedFields("f1")
    static class MyMap implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        @Override
        public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
            return new Tuple2<>(value.f0, value.f1 * 8);
        }
    }

}

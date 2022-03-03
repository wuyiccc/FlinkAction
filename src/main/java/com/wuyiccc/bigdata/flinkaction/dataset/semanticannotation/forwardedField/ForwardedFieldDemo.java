package com.wuyiccc.bigdata.flinkaction.dataset.semanticannotation.forwardedField;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author wuyiccc
 * @date 2022/3/4 7:30
 * 使用函数类注释生命转发字段信息
 */
public class ForwardedFieldDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, Integer>> input = env.fromElements(
                Tuple2.of(1, 2)
        );
        input.map(new MyMap()).print();

    }


    @FunctionAnnotation.ForwardedFields("f0->f2")
    static class MyMap implements MapFunction<Tuple2<Integer, Integer>, Tuple3<String, Integer, Integer>> {

        @Override
        public Tuple3<String, Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {

            return new Tuple3<>("foo", value.f1 * 8, value.f0);
        }
    }
}

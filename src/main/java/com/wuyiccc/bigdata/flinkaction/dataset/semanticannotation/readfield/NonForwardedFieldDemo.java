package com.wuyiccc.bigdata.flinkaction.dataset.semanticannotation.readfield;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * @author wuyiccc
 * @date 2022/3/4 7:40
 * 声明读取字段的信息
 */
public class NonForwardedFieldDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Integer, Integer, Integer>> input = env.fromElements(
                    Tuple4.of(1, 2, 3, 4)
        );
        input.map(new MyMap()).print();
    }

    // f0和f3由该函数进行读取和评估
    @FunctionAnnotation.ReadFields("f0; f3")
    static class MyMap implements MapFunction<Tuple4<Integer, Integer, Integer, Integer>, Tuple2<Integer, Integer>> {

        @Override
        public Tuple2<Integer, Integer> map(Tuple4<Integer, Integer, Integer, Integer> value) throws Exception {
           if (value.f0 == 2) {
               return new Tuple2<>(value.f0, value.f1);
           } else {
               return new Tuple2<>(value.f3 + 8, value.f1 + 8);
           }
        }
    }
}

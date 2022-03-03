package com.wuyiccc.bigdata.flinkaction.transformations.datastream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wuyiccc
 * @date 2022/3/3 23:16
 * 使用Reduce操作键控流
 */
public class MyReduceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> source = senv.fromElements(
                new Tuple2<>("A", 1),
                new Tuple2<>("B", 3),
                new Tuple2<>("C", 6),
                new Tuple2<>("A", 5),
                new Tuple2<>("B", 8),
                new Tuple2<>("B", 5)
        );

        DataStream<Tuple2<String, Integer>> reduce = source.keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
               return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        });
        reduce.print();
        senv.execute("Reduce Demo");
    }

}

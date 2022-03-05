package com.wuyiccc.bigdata.flinkaction.datastream.window.windowfunction;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wuyiccc
 * @date 2022/3/5 8:40
 */
public class FoldFunctionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Long>> input = env.fromElements(
                new Tuple2("BMW", 2L),
                new Tuple2("BMW", 2L),
                new Tuple2("BMW", 2L),
                new Tuple2("Tesla", 3L),
                new Tuple2("Tesla", 4L),
                new Tuple2("Tesla", 4L)
        );
        DataStream<String> output = input.keyBy(0)
                .countWindow(3)
                .fold("", new FoldFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public String fold(String accumulator, Tuple2<String, Long> value) throws Exception {
                        return accumulator + value.f1;
                    }
                });
        output.print();
        env.execute();
    }
}

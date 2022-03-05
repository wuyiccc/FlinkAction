package com.wuyiccc.bigdata.flinkaction.datastream.window.windowfunction;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wuyiccc
 * @date 2022/3/5 8:24
 */
public class ReduceFunctionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Long>> input = env.fromElements(
                new Tuple2("BMW", 2L),
                new Tuple2("BMW", 2L),
                new Tuple2("Tesla", 3L),
                new Tuple2("Tesla", 4L)
        );
        DataStream<Tuple2<String, Long>> output = input.keyBy(0)
                .countWindow(2)
                // 汇总了窗口中所有元素的第二个字段
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {

                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                });
        output.print();
        env.execute();
    }
}

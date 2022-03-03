package com.wuyiccc.bigdata.flinkaction.transformations.datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wuyiccc
 * @date 2022/3/4 0:36
 * 1. DataStream归零迭代运算
 */
public class IterationDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 0, 1, 2, 3, 4
        DataStream<Long> input = env.generateSequence(0, 4);
        IterativeStream<Long> iterateStream = input.iterate();
        SingleOutputStreamOperator<Long> zero = iterateStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1;
            }
        });

        SingleOutputStreamOperator<Long> greaterThanZero = zero.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });
        iterateStream.closeWith(greaterThanZero);
        SingleOutputStreamOperator<Long> lessThanZero = zero.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value <= 0;
            }
        });
        zero.print("IterationDemo");
        env.execute();
    }
}

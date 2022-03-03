package com.wuyiccc.bigdata.flinkaction.transformations.datastream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.expressions.In;

/**
 * @author wuyiccc
 * @date 2022/3/3 21:46
 * 使用Union算子连接多个数据源
 */
public class MyUnionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> source1 = senv.fromElements(
                new Tuple2<>("Honda", 15),
                new Tuple2<>("CROWN", 25)
        );
        DataStream<Tuple2<String, Integer>> source2 = senv.fromElements(
                new Tuple2<>("BMW", 35),
                new Tuple2<>("Tesla",40)
        );
        DataStream<Tuple2<String, Integer>> source3 = senv.fromElements(
                new Tuple2<>("Rolls-Royce", 300),
                new Tuple2<>("Tesla", 330)
        );
        DataStream<Tuple2<String, Integer>> union = source1.union(source2, source3);
        union.print();
        senv.execute();
    }
}

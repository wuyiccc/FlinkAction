package com.wuyiccc.bigdata.flinkaction.transformations.datastream;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wuyiccc
 * @date 2022/3/3 21:53
 * 使用Connect算子连接不同类型的数据源
 */
public class MyConnectDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple1<String>> source1 = senv.fromElements(
                new Tuple1<>("Honda"),
                new Tuple1<>("CROWN")
        );
        DataStream<Tuple2<String, Integer>> source2 = senv.fromElements(
                new Tuple2<>("BMW", 35),
                new Tuple2<>("Tesla", 40)
        );
        ConnectedStreams<Tuple1<String>, Tuple2<String, Integer>>  connectedStreams = source1.connect(source2);
        connectedStreams.getFirstInput().print("union");
        connectedStreams.getSecondInput().print("union");
        senv.execute();
    }
}

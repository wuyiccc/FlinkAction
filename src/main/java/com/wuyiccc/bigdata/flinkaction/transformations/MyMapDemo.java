package com.wuyiccc.bigdata.flinkaction.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.expressions.In;

/**
 * @author wuyiccc
 * @date 2022/3/3 0:45
 */
public class MyMapDemo {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        DataStream<Integer> dataStream = sEnv.fromElements(4, 1, 7).map(x -> x + 8);
        dataStream.print("Map");
        sEnv.execute("Map Job");
    }
}

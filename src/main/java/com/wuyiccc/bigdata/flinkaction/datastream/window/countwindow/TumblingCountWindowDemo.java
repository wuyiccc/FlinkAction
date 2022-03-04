package com.wuyiccc.bigdata.flinkaction.datastream.window.countwindow;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wuyiccc
 * @date 2022/3/5 7:34
 * 滚动计数窗口
 */
public class TumblingCountWindowDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("S1",1),
                Tuple2.of("S2",2),
                Tuple2.of("S1",3),
                Tuple2.of("S2",4),
                Tuple2.of("S2",5),
                Tuple2.of("S2",6),
                Tuple2.of("S3",7),
                Tuple2.of("S3",8),
                Tuple2.of("S3",9)
        );
        input.keyBy(0)
                // 计数窗口
                .countWindow(3)
                .sum(1)
                .print();

        env.execute();
    }
}

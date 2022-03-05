package com.wuyiccc.bigdata.flinkaction.datastream.window.windowfunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * @author wuyiccc
 * @date 2022/3/5 8:44
 */
public class ProcessWindowFunctionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<Tuple2<String, Long>> input = env.fromElements(
                new Tuple2<>("BMW", 1L),
                new Tuple2<>("BMW", 2L),
                new Tuple2<>("Tesla", 3L),
                new Tuple2<>("BMW", 3L),
                new Tuple2<>("Tesla", 4L)
        );

        DataStream<String> output = input.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<String, Long> element) {
                        return element.f1 + 2000;
                    }
                }).keyBy(t -> t.f0)
                .timeWindow(Time.seconds(2))
                .process(new MyProcessWindowFunction());
        output.print();
        env.execute();

    }
}

class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {


    @Override
    public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
        long count = 0;
        for (Tuple2<String, Long> in : elements) {
            count++;
        }
        out.collect("窗口信息: " + context.window() + "元素数量: " + count);
    }
}

package com.wuyiccc.bigdata.flinkaction.datastream.window.timewindow;

import com.wuyiccc.bigdata.flinkaction.demo.utils.Splitter;
import com.wuyiccc.bigdata.flinkaction.demo.utils.source.MySource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author wuyiccc
 * @date 2022/3/5 0:16
 * 实现滚动时间窗口
 */
public class TumblingTimeWindowDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.addSource(new MySource());
        DataStream<Tuple2<String, Integer>> output = input
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(3))
                .sum(1);

        output.print("window");
        env.execute("WordCount");
    }
}

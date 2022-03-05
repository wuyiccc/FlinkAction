package com.wuyiccc.bigdata.flinkaction.datastream.window.sessionwindow;

import com.wuyiccc.bigdata.flinkaction.demo.utils.Splitter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author wuyiccc
 * @date 2022/3/5 7:59
 * 实现会话窗口
 */
public class SessionWindowDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.addSource(new MySource());
        DataStream<Tuple2<String, Integer>> output = input
                .flatMap(new Splitter())
                .keyBy(0)
                // 如果超过2s没有事件, 则计算进入窗口内的总数
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
                .sum(1);
        output.print("window");
        env.execute("WordCount");
    }
}

package com.wuyiccc.bigdata.flinkaction.datastream.trigger;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;



public class TumblingWindowDemo {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取自定义的数据流
        DataStream<String> input = env.addSource(new MySource());
        DataStream<Tuple2<String, Integer>> output=input
                .flatMap(new Splitter())
                .keyBy(0)

                //指定窗口时间
                .timeWindow(Time.seconds(15)).trigger(new MyTrigger())
                .sum(1);

        //打印数据到控制台
        output.print("window");
        //执行任务操作。因为flink是懒加载的，所以必须调用execute方法才会执行
        env.execute("WordCount");
    }

    //使用FlatMapFunction函数分割字符串
    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}

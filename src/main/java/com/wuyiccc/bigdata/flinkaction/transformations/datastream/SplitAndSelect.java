package com.wuyiccc.bigdata.flinkaction.transformations.datastream;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wuyiccc
 * @date 2022/3/3 23:28
 * 使用Split算子子拆分数据流, 并使用Select选择拆分后的数据流
 */
public class SplitAndSelect {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> input = sEnv.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
        sEnv.setParallelism(1);
        SplitStream<Integer> split = input.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                List<String> output = new ArrayList<>();
                if (value % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        });
        DataStream<Integer> even = split.select("even");
        DataStream<Integer> odd = split.select("odd");
        DataStream<Integer> all = split.select("even", "odd");
        even.print("even流");
        odd.print("odd流");
        all.print("all流");
        sEnv.execute();
    }
}

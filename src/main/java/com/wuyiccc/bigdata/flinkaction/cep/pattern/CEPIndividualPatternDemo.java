package com.wuyiccc.bigdata.flinkaction.cep.pattern;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author wuyiccc
 * @date 2022/3/5 22:51
 */
public class CEPIndividualPatternDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.fromElements("a1", "c", "b4", "a2", "b2", "a3");

        // 定义匹配模式
        Pattern<String, ?> pattern = Pattern.<String>begin("start")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                       return value.startsWith("a");
                    }
                });

        // 执行Pattern
        PatternStream<String> patternStream = CEP.pattern(input, pattern);
        DataStream<String> result = patternStream
                .process(new PatternProcessFunction<String, String>() {
                    @Override
                    public void processMatch(Map<String, List<String>> map, Context context, Collector<String> collector) throws Exception {
                        System.out.println(map);
                    }
                });
        result.print();
        env.execute();
    }

}

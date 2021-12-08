package com.wuyiccc.bigdata.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author wuyiccc
 * @date 2021/12/8 23:02
 */
public class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {


    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        for (String word : value.split(" ")) {
            out.collect(new Tuple2<>(word, 1));
        }
    }
}

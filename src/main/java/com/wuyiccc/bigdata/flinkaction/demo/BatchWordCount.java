package com.wuyiccc.bigdata.flinkaction.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author wuyiccc
 * @date 2021/12/8 22:19
 * 统计有界数据集中单词的数量
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {

        // 获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 创建源数据
        DataSource<String> elementsSource = env.fromElements("Flink batch demo", "batch demo", "demo");
        // 转换数据
        DataSet<Tuple2<String, Integer>> res = elementsSource.flatMap(new LineSplitter()).groupBy(0).sum(1);
        // Batch批处理操作的print, 在源码内部会调用execute()方法, 如果再次执行execute方法则会报错
        res.print();
    }

    static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : value.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}

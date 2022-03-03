package com.wuyiccc.bigdata.flinkaction.transformations.datasetapi.demo14;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @author wuyiccc
 * @date 2022/3/3 20:11
 * 使用键表达式对POJO数据集进行分组, 通过Reduce算子进行聚合
 */
public class ReduceDataSetGroupedByKeyExpression {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



        DataSet<WC> words = env.fromElements(
                new WC("BMW", 1),
                new WC("Tesla", 1),
                new WC("Tesla", 9),
                new WC("Rolls-Royce", 1)
        );
        DataSet<WC> wordCounts = words.groupBy("word").reduce(new WordCounter());
        wordCounts.print();
    }


    public static class WC {
        public String word;
        public int count;

        public WC() {
        }

        public WC(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

    public static class WordCounter implements ReduceFunction<WC> {

        @Override
        public WC reduce(WC value1, WC value2) throws Exception {
            return new WC(value1.word, value1.count + value2.count);
        }
    }
}

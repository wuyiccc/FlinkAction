package com.wuyiccc.bigdata.flinkaction.transformations.datasetapi.demo14;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author wuyiccc
 * @date 2022/3/3 20:21
 * 使用键选择器对POJO数据集进行分组和聚合
 */
public class ReduceDataSetGroupedByKeySelectorFunction {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<WC> words = env.fromElements(
                new WC("BMW", 1),
                new WC("Tesla", 1),
                new WC("Tesla", 9),
                new WC("Rolls-Royce", 1)
        );

        DataSet<WC> wordCounts = words.groupBy(new SelectWord())
                .reduce(new WordCounter());
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

    public static class SelectWord implements KeySelector<WC, String> {

        @Override
        public String getKey(WC value) throws Exception {
            return value.word;
        }
    }

    public static class WordCounter implements ReduceFunction<WC> {

        @Override
        public WC reduce(WC value1, WC value2) throws Exception {

            return new WC(value1.word, value1.count + value2.count);
        }
    }
}

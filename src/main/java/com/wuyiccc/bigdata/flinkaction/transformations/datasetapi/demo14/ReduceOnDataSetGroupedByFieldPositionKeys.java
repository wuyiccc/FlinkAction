package com.wuyiccc.bigdata.flinkaction.transformations.datasetapi.demo14;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author wuyiccc
 * @date 2022/3/3 20:46
 * 使用First-n算子返回数据集的前n个元素
 */
public class ReduceOnDataSetGroupedByFieldPositionKeys {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String, Integer>> in = env.fromElements(
                Tuple2.of("BMW", 100),
                Tuple2.of("Tesla", 35),
                Tuple2.of("Tesla", 55),
                Tuple2.of("Tesla", 80),
                Tuple2.of("Rolls-Royce", 300),
                Tuple2.of("BMW", 40),
                Tuple2.of("BMW", 45),
                Tuple2.of("BMW", 80)
        );
        // 返回前2个元素
        DataSet<Tuple2<String, Integer>> out1 = in.first(2);
        // 返回每个分组的前两个元素
        DataSet<Tuple2<String, Integer>> out2 = in.groupBy(0).first(2);
        // 根据字段2进行升序排列, 返回前两个元素
        DataSet<Tuple2<String, Integer>> out3 = in.groupBy(0).sortGroup(1, Order.ASCENDING)
                .first(2);

        out1.print();
        System.out.println("----------------");
        out2.print();
        System.out.println("-----------------");
        out3.print();

    }
}

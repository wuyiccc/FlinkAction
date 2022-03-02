package com.wuyiccc.bigdata.flinkaction.demo;

import com.wuyiccc.bigdata.flinkaction.demo.pojo.MyOrder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author wuyiccc
 * @date 2021/12/9 7:39
 */
public class TableBatchDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSource<MyOrder> input = env.fromElements(
                new MyOrder(1L, "BMW", 1),
                new MyOrder(2L, "Tesla", 8),
                new MyOrder(2L, "Tesla", 8),
                new MyOrder(3L, "Rolls-Royce", 20));
        Table table = tEnv.fromDataSet(input);


        Table filter = table.where($("amount").isGreaterOrEqual(8));

        DataSet<MyOrder> res = tEnv.toDataSet(filter, MyOrder.class);

        res.print();
    }
}

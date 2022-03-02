package com.wuyiccc.bigdata.flinkaction.demo;

import com.wuyiccc.bigdata.flinkaction.demo.pojo.MyOrder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author wuyiccc
 * @date 2021/12/9 22:51
 */
public class SQLBatchDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        DataSet<MyOrder> input = env.fromElements(
                new MyOrder(1L, "BMW", 1),
                new MyOrder(2L, "Tesla", 8),
                new MyOrder(2L, "Tesla", 8),
                new MyOrder(3L, "Rolls-Royce", 20));
        tEnv.createTemporaryView("MyOrder", input, $("id"), $("product"), $("amount"));
        Table table = tEnv.sqlQuery("SELECT product, SUM(amount) as amount FROM MyOrder GROUP BY product");
        tEnv.toDataSet(table, Row.class).print();
    }
}

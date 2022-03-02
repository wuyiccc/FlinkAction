package com.wuyiccc.bigdata.flinkaction.transformations.filter;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;

/**
 * @author wuyiccc
 * @date 2022/3/3 0:52
 * 使用Filter算子过滤数据
 */
public class MyFilterDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> input = env.fromElements(-1, -2, -3, 1, 2, 417);
        DataSet<Integer> ds = input.filter(new MyFilterFunction());
        ds.print();
    }
}

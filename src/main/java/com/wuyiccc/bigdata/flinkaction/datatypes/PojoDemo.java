package com.wuyiccc.bigdata.flinkaction.datatypes;

import com.wuyiccc.bigdata.flinkaction.datatypes.pojo.MyCar;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;

/**
 * @author wuyiccc
 * @date 2022/3/2 23:56
 * 在Flink中使用Java的POJO类
 */
public class PojoDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<MyCar> input = env.fromElements(
                new MyCar("BMW", 3000),
                new MyCar("Tesla", 4000),
                new MyCar("Tesla", 400),
                new MyCar("Rolls-Rocyle", 200)
        );

        FilterOperator<MyCar> output = input.filter(new FilterFunction<MyCar>() {
            @Override
            public boolean filter(MyCar value) throws Exception {
                return value.getAmount() > 1000;
            }
        });
        output.print();
    }
}

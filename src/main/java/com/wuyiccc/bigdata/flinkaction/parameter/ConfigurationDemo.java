package com.wuyiccc.bigdata.flinkaction.parameter;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * @author wuyiccc
 * @date 2022/3/2 22:37
 */
public class ConfigurationDemo {

    public static void main(String[] args) throws Exception {
        // 获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 加载或创建源数据
        DataSet<Integer> input = env.fromElements(1, 2, 3, 5, 10, 12, 15, 16);
        // 用Configuration类存储参数
        Configuration configuration = new Configuration();
        configuration.setInteger("limit", 8);
        input.filter(new RichFilterFunction<Integer>() {
            private int limit;

            @Override
            public void open(Configuration parameters) throws Exception {
                limit = parameters.getInteger("limit", 0);
            }

            @Override
            public boolean filter(Integer value) throws Exception {
                // 返回大于limit的值
                return value > limit;
            }
        }).withParameters(configuration).print();

    }
}

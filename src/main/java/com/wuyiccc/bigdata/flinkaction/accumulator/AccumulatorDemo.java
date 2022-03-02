package com.wuyiccc.bigdata.flinkaction.accumulator;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.Map;

/**
 * @author wuyiccc
 * @date 2022/3/2 23:10
 */
public class AccumulatorDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> input = env.fromElements("BMW", "Tesla", "Rolls-Royce");
        DataSet<String> result = input.map(new RichMapFunction<String, String>() {
            // 创建累加器
            IntCounter intCounter = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                // 注册累加器
                getRuntimeContext().addAccumulator("myAccumulatorName", intCounter);
            }

            @Override
            public String map(String value) throws Exception {
                // 使用普通累加器, 如果设置了多个并行度, 则普通的求和结果不准确
                intCounter.add(1);
                return value;
            }
        });



        // 输出数据, 并设置并行度为1
        result.writeAsText("./file/file.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        JobExecutionResult jobExecutionResult = env.execute("myJob");

        int accumulatorResult = jobExecutionResult.getAccumulatorResult("myAccumulatorName");
        System.out.println(accumulatorResult);
    }
}

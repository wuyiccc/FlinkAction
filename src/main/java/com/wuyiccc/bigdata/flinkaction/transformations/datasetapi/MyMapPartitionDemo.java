package com.wuyiccc.bigdata.flinkaction.transformations.datasetapi;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @author wuyiccc
 * @date 2022/3/3 19:45
 * 使用MAPPartition算子统计数据集的分区计数
 */
public class MyMapPartitionDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> textLines = env.fromElements("BMW", "Tesla", "Rolls-Roycle");
        DataSet<Long> counts = textLines.mapPartition(new PartitionCounter());

        counts.print();
    }
}

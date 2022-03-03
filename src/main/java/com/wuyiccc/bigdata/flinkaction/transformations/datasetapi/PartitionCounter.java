package com.wuyiccc.bigdata.flinkaction.transformations.datasetapi;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.Collector;

/**
 * @author wuyiccc
 * @date 2022/3/3 20:03
 */
public class PartitionCounter implements MapPartitionFunction<String, Long> {


    @Override
    public void mapPartition(Iterable<String> values, Collector<Long> out) throws Exception {
        long i = 0;
        for (String value : values) {
            i++;
        }
        out.collect(i);;
    }
}

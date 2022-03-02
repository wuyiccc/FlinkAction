package com.wuyiccc.bigdata.flinkaction.transformations.filter;

import org.apache.flink.api.common.functions.RichFilterFunction;

/**
 * @author wuyiccc
 * @date 2022/3/3 0:54
 */
public class MyFilterFunction extends RichFilterFunction<Integer> {

    @Override
    public boolean filter(Integer value) throws Exception {
        return value > 0;
    }
}

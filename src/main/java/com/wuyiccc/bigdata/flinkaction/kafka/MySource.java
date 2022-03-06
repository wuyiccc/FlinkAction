package com.wuyiccc.bigdata.flinkaction.kafka;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MySource implements SourceFunction<String> {//1

    private long count = 1L;
    private boolean isRunning = true;

    /**
     * 在run方法中实现一个循环来产生数据
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {


        while (isRunning) {
            ctx.collect("消息"+count);
            count+=1;
            Thread.sleep(1000);
        }
    }

    //cancel方法代表取消执行
    @Override
    public void cancel() {
        isRunning = false;
    }
}
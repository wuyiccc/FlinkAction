package com.wuyiccc.bigdata.flinkaction.datastream.window.sessionwindow;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author wuyiccc
 * @date 2022/3/5 7:53
 */
public class MySource implements SourceFunction<String> {
    private long count = 1L;
    private boolean isRunning = true;
    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        List<String> stringList = new ArrayList<>();
        stringList.add("world");
        stringList.add("Flink");
        stringList.add("Steam");
        stringList.add("Batch");
        stringList.add("Table");
        stringList.add("SQL");
        stringList.add("hello");
        int size = stringList.size();

        while (isRunning) {
            int i = new Random().nextInt(size);
            ctx.collect(stringList.get(i));
            System.out.println("Source:" + stringList.get(i));
            int rt = i * 1000;
            System.out.println("延迟时间:" + rt);
            Thread.sleep(rt);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
